"""质控放行（QC release）领域逻辑。

数据员在本地副本上把核查过的站点读数整批冻结，逐条确认异常后放行给离线分析。

核心不变量
----------
1. 建批时把该站点当前的 stations.correction 与全部 readings 行**整体冻结**到
   qc_batch_readings，异常判定 value + correction 基于冻结值——之后源端怎么变
   都不影响批内内容与结论。
2. 失效与源事务的两表落库在**同一个 SQLite 事务**内提交（见
   worker.ChangeConsumer._commit），因此“数据已变但批次仍有效”这种中间状态
   不可能被任何读者观察到；在途审核与更正交错时，放行用条件 UPDATE 兜底，
   过期批次永远无法放行。
3. 所有写操作都是单事务，失败即回滚，不留半份审核/放行；进程重启后结论直接从
   SQLite 恢复。
4. 供数只返回 valid 且 released 的批次；批次状态与明细始终在同一个读事务内取出，
   二者必然对应同一已提交快照。
5. 副本未发布（初始全量未完成 / 重置后重建中）不能建批。
6. 副本重置不会删除任何历史：仅把仍在途/已放行的批次标记为 invalidated，
   原始输入、确认记录与失效原因全部保留。
"""
import threading
from datetime import datetime, timezone

from . import store

# ---------------------------------------------------------------- 失效原因
REASON_STATION_CORRECTION = "站点校正值已提交变更"
REASON_READING_CHANGED = "站点读数内容或成员已提交变更"
REASON_STATION_MOVED = "迁站：读数成员在站点间迁移"
REASON_REPLICA_RESET = "副本重置：全部旧批失效"
REASON_SNAPSHOT_REBUILD = "全量重建：副本整体替换，旧批失效"

VALID = "valid"
RELEASED = "released"
INVALIDATED = "invalidated"

QC_SCHEMA = """
CREATE TABLE IF NOT EXISTS qc_batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id      INTEGER NOT NULL,
    lower_limit     INTEGER NOT NULL,
    upper_limit     INTEGER NOT NULL,
    correction      INTEGER NOT NULL,           -- 建批时冻结的 stations.correction
    status          TEXT NOT NULL DEFAULT 'valid',  -- valid | released | invalidated
    invalid_reason  TEXT,
    created_at      TEXT NOT NULL,
    released_at     TEXT,
    invalidated_at  TEXT
);
CREATE TABLE IF NOT EXISTS qc_batch_readings (
    batch_id        INTEGER NOT NULL REFERENCES qc_batches(id),
    reading_id      INTEGER NOT NULL,
    station_id      INTEGER NOT NULL,           -- 冻结时的归属站点
    value           INTEGER NOT NULL,           -- 冻结时的 readings.value
    corrected_value INTEGER NOT NULL,           -- 冻结 value + 冻结 correction
    is_anomaly      INTEGER NOT NULL,           -- 1 超出 [lower, upper]，否则 0
    PRIMARY KEY (batch_id, reading_id)
);
CREATE TABLE IF NOT EXISTS qc_confirmations (
    batch_id        INTEGER NOT NULL REFERENCES qc_batches(id),
    reading_id      INTEGER NOT NULL,
    confirmed_at    TEXT NOT NULL,
    PRIMARY KEY (batch_id, reading_id)
);
CREATE INDEX IF NOT EXISTS idx_qc_batches_station ON qc_batches(station_id);
CREATE INDEX IF NOT EXISTS idx_qc_readings_batch ON qc_batch_readings(batch_id);
"""

_write_lock = threading.Lock()


class QcError(Exception):
    """业务校验失败（映射为 HTTP 409）。"""


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ------------------------------------------------------------- 建表 / 重置
def init_qc_tables(conn):
    conn.executescript(QC_SCHEMA)


def invalidate_all_on_reset(conn, reason=REASON_REPLICA_RESET):
    """副本整体替换：全部批次永久失效，历史行（输入/确认）原样保留。

    已失效批次保留原原因与原失效时间，仅追加本次原因；
    在途/已放行批次在此刻变为失效并记录失效时间。
    """
    now = utcnow()
    # 在途 / 已放行 → 失效（记录失效时间）
    conn.execute(
        "UPDATE qc_batches SET status = ?, invalid_reason = ?, invalidated_at = ? "
        "WHERE status IN (?, ?)",
        (INVALIDATED, reason, now, VALID, RELEASED),
    )
    # 此前已失效的批次：追加本次原因，不动原失效时间
    conn.execute(
        "UPDATE qc_batches "
        "SET invalid_reason = invalid_reason || '; ' || ? "
        "WHERE status = ? AND invalid_reason IS NOT NULL "
        "AND invalid_reason NOT LIKE '%' || ? || '%'",
        (reason, INVALIDATED, reason),
    )


# ----------------------------------------------------------------- 辅助
def _mark_invalid(conn, station_ids, reason):
    """把给定站点仍处于 valid/released 的批次标记失效（幂等，可叠加原因）。"""
    if not station_ids:
        return
    now = utcnow()
    marks = ",".join("?" for _ in station_ids)
    conn.execute(
        f"UPDATE qc_batches "
        f"SET status = ?, "
        f"    invalid_reason = CASE "
        f"        WHEN invalid_reason IS NULL OR invalid_reason = '' THEN ? "
        f"        ELSE invalid_reason || '; ' || ? END, "
        f"    invalidated_at = ? "
        f"WHERE station_id IN ({marks}) AND status IN (?, ?)",
        (INVALIDATED, reason, reason, now, *station_ids, VALID, RELEASED),
    )


def _reading_station(conn, reading_id):
    row = conn.execute(
        "SELECT station_id FROM readings WHERE id = ?", (reading_id,)
    ).fetchone()
    return row[0] if row else None


def invalidate_for_changes(conn, changes):
    """按一个源事务内的两表变更计算受影响站点并使相关批次失效。

    与副本数据写入在同一个事务内调用（且在 apply_change 之前），保证失效和
    两表落库一起生效，并可从 SQLite 读到变更前的旧归属：
      - stations UPDATE（correction 变化或主键迁移）→ 旧站点与新站点
      - readings DELETE                          → 该读数原属站点
      - readings UPDATE（station_id/value 变化） → 旧站点与新站点
      - readings INSERT                          → 新站点（成员增加）
      - stations INSERT                          → 无影响（新站没有批次）
    test_decoding 在 REPLICA IDENTITY DEFAULT 下，非主键列 UPDATE/DELETE 不携带
    旧的非键列，因此 readings 的旧 station_id 一律在落库前回查副本当前行。
    """
    affected = set()
    for table, op, old, new in changes:
        if table == "stations":
            if op == "UPDATE":
                # 普通 UPDATE 解码器只给新行（old 为 None）；主键变化时两者都有。
                affected.add(new["id"])
                if old is not None:
                    affected.add(old["id"])
            elif op == "DELETE" and old is not None:
                affected.add(old["id"])
        elif table == "readings":
            if op == "DELETE":
                sid = (old or {}).get("station_id")
                if sid is None and old is not None:
                    sid = _reading_station(conn, old["id"])
                if sid is not None:
                    affected.add(sid)
            elif op == "UPDATE":
                old_sid = (old or {}).get("station_id")
                if old_sid is None:
                    lookup_id = (old or {}).get("id", new["id"])
                    old_sid = _reading_station(conn, lookup_id)
                if old_sid is not None:
                    affected.add(old_sid)
                affected.add(new["station_id"])
            elif op == "INSERT":
                affected.add(new["station_id"])
    if not affected:
        return
    # 迁站（读数 station_id 变化）影响旧、新两个站点，单独记原因；
    # 回查得到的旧归属与新归属不同即判定为迁站。
    moved_stations = set()
    for table, op, old, new in changes:
        if table != "readings" or op != "UPDATE":
            continue
        old_sid = (old or {}).get("station_id")
        if old_sid is None:
            lookup_id = (old or {}).get("id", new["id"])
            old_sid = _reading_station(conn, lookup_id)
        if old_sid is not None and old_sid != new["station_id"]:
            moved_stations.add(old_sid)
            moved_stations.add(new["station_id"])
    if moved_stations:
        _mark_invalid(conn, list(moved_stations), REASON_STATION_MOVED)
        affected -= moved_stations
    # 同事务同时触及站点校正与读数时，两类原因分别叠加到各自站点。
    station_corr = {
        new["id"]
        for table, op, old, new in changes
        if table == "stations" and op == "UPDATE"
    }
    _mark_invalid(conn, list(station_corr & affected), REASON_STATION_CORRECTION)
    _mark_invalid(conn, list(affected - station_corr), REASON_READING_CHANGED)


# --------------------------------------------------------------- 对外操作
def create_batch(station_id, lower_limit, upper_limit):
    """选站 + 整数上下限 → 冻结该站全部读数与 correction，生成质控批。

    在单个 SQLite 事务内完成（BEGIN IMMEDIATE 拿到写锁，保证冻结快照与后续
    失效判定串行于写入线程），失败回滚不留任何痕迹。
    """
    if lower_limit > upper_limit:
        raise QcError("下限不能大于上限")
    with _write_lock:
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if not store.published():
                raise QcError("副本未发布（初始全量未完成），不能建批")
            station = conn.execute(
                "SELECT correction FROM stations WHERE id = ?", (station_id,)
            ).fetchone()
            if station is None:
                raise QcError(f"站点 {station_id} 不存在于当前副本")
            correction = station[0]
            rows = conn.execute(
                "SELECT id, station_id, value FROM readings "
                "WHERE station_id = ? ORDER BY id",
                (station_id,),
            ).fetchall()
            now = utcnow()
            cur = conn.execute(
                "INSERT INTO qc_batches(station_id, lower_limit, upper_limit, "
                "correction, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (station_id, lower_limit, upper_limit, correction, VALID, now),
            )
            batch_id = cur.lastrowid
            for reading_id, sid, value in rows:
                corrected = value + correction
                is_anom = 1 if (corrected < lower_limit or corrected > upper_limit) else 0
                conn.execute(
                    "INSERT INTO qc_batch_readings(batch_id, reading_id, station_id, "
                    "value, corrected_value, is_anomaly) VALUES (?, ?, ?, ?, ?, ?)",
                    (batch_id, reading_id, sid, value, corrected, is_anom),
                )
            conn.commit()
            return get_batch(batch_id)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def confirm_anomaly(batch_id, reading_id):
    """逐条确认一条异常读数。整批异常全部确认后才允许放行。"""
    with _write_lock:
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute(
                "SELECT status FROM qc_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise QcError(f"批次 {batch_id} 不存在")
            if batch[0] != VALID:
                raise QcError(f"批次状态为 {batch[0]}，不能确认")
            row = conn.execute(
                "SELECT is_anomaly FROM qc_batch_readings "
                "WHERE batch_id = ? AND reading_id = ?",
                (batch_id, reading_id),
            ).fetchone()
            if row is None:
                raise QcError("该读数不属于此批次")
            if row[0] != 1:
                raise QcError("该读数不是异常读数，无需确认")
            exists = conn.execute(
                "SELECT 1 FROM qc_confirmations WHERE batch_id = ? AND reading_id = ?",
                (batch_id, reading_id),
            ).fetchone()
            if exists:
                raise QcError("该异常已确认，请勿重复确认")
            conn.execute(
                "INSERT INTO qc_confirmations(batch_id, reading_id, confirmed_at) "
                "VALUES (?, ?, ?)",
                (batch_id, reading_id, utcnow()),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return get_batch(batch_id)


def release_batch(batch_id):
    """放行：仅当批次仍 valid 且全部异常已逐条确认。"""
    with _write_lock:
        conn = store.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            batch = conn.execute(
                "SELECT status FROM qc_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise QcError(f"批次 {batch_id} 不存在")
            if batch[0] == INVALIDATED:
                reason = conn.execute(
                    "SELECT invalid_reason FROM qc_batches WHERE id = ?", (batch_id,)
                ).fetchone()[0]
                raise QcError(f"批次已失效，不能放行（原因：{reason}）")
            if batch[0] == RELEASED:
                raise QcError("批次已放行，请勿重复操作")
            remaining = conn.execute(
                "SELECT COUNT(*) FROM qc_batch_readings r "
                "WHERE r.batch_id = ? AND r.is_anomaly = 1 AND NOT EXISTS ("
                "    SELECT 1 FROM qc_confirmations c "
                "    WHERE c.batch_id = r.batch_id AND c.reading_id = r.reading_id)",
                (batch_id,),
            ).fetchone()[0]
            if remaining:
                raise QcError(f"仍有 {remaining} 条异常未确认，不能放行")
            # 条件 UPDATE 兜底：即便失效在本事务期间排队，状态已非 valid，
            # 放行影响 0 行，过期批次不可能被供数。
            cur = conn.execute(
                "UPDATE qc_batches SET status = ?, released_at = ? "
                "WHERE id = ? AND status = ?",
                (RELEASED, utcnow(), batch_id, VALID),
            )
            if cur.rowcount != 1:
                raise QcError("批次状态已变化，放行被拒绝")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        return get_batch(batch_id)


# ------------------------------------------------------------------- 查询
def _batch_row(conn, batch_id):
    row = conn.execute(
        "SELECT id, station_id, lower_limit, upper_limit, correction, status, "
        "invalid_reason, created_at, released_at, invalidated_at "
        "FROM qc_batches WHERE id = ?",
        (batch_id,),
    ).fetchone()
    if row is None:
        raise QcError(f"批次 {batch_id} 不存在")
    return row


def _batch_dict(conn, row, with_details=False):
    (bid, station_id, lower, upper, correction, status, reason,
     created_at, released_at, invalidated_at) = row
    total = conn.execute(
        "SELECT COUNT(*) FROM qc_batch_readings WHERE batch_id = ?", (bid,)
    ).fetchone()[0]
    anomalies = conn.execute(
        "SELECT COUNT(*) FROM qc_batch_readings WHERE batch_id = ? AND is_anomaly = 1",
        (bid,),
    ).fetchone()[0]
    confirmed = conn.execute(
        "SELECT COUNT(*) FROM qc_confirmations WHERE batch_id = ?", (bid,)
    ).fetchone()[0]
    d = {
        "id": bid,
        "station_id": station_id,
        "lower_limit": lower,
        "upper_limit": upper,
        "correction": correction,
        "status": status,
        "invalid_reason": reason,
        "created_at": created_at,
        "released_at": released_at,
        "invalidated_at": invalidated_at,
        "reading_count": total,
        "anomaly_count": anomalies,
        "confirmed_count": confirmed,
    }
    if with_details:
        d["readings"] = [
            {
                "reading_id": r[0],
                "station_id": r[1],
                "value": r[2],
                "corrected_value": r[3],
                "is_anomaly": bool(r[4]),
                "confirmed": bool(r[5]),
                "confirmed_at": r[6],
            }
            for r in conn.execute(
                "SELECT r.reading_id, r.station_id, r.value, r.corrected_value, "
                "r.is_anomaly, CASE WHEN c.reading_id IS NULL THEN 0 ELSE 1 END, "
                "c.confirmed_at "
                "FROM qc_batch_readings r "
                "LEFT JOIN qc_confirmations c "
                "  ON c.batch_id = r.batch_id AND c.reading_id = r.reading_id "
                "WHERE r.batch_id = ? ORDER BY r.reading_id",
                (bid,),
            ).fetchall()
        ]
    return d


def get_batch(batch_id):
    """状态与明细取自同一已提交快照（单个读事务）。"""
    conn = store.connect()
    try:
        conn.execute("BEGIN")
        row = _batch_row(conn, batch_id)
        d = _batch_dict(conn, row, with_details=True)
        conn.commit()
        return d
    finally:
        conn.close()


def list_batches():
    """全部批次概览（含已失效历史），单读事务一致读取。"""
    conn = store.connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT id, station_id, lower_limit, upper_limit, correction, status, "
            "invalid_reason, created_at, released_at, invalidated_at "
            "FROM qc_batches ORDER BY id"
        ).fetchall()
        out = [_batch_dict(conn, row) for row in rows]
        conn.commit()
        return out
    finally:
        conn.close()


def get_released_batch(batch_id):
    """供数：仅返回 valid 且 released 的批次（含全部冻结明细）。"""
    conn = store.connect()
    try:
        conn.execute("BEGIN")
        row = conn.execute(
            "SELECT id, station_id, lower_limit, upper_limit, correction, status, "
            "invalid_reason, created_at, released_at, invalidated_at "
            "FROM qc_batches WHERE id = ? AND status = ?",
            (batch_id, RELEASED),
        ).fetchone()
        if row is None:
            raise QcError(f"批次 {batch_id} 不是有效已放行批次")
        d = _batch_dict(conn, row, with_details=True)
        conn.commit()
        return d
    finally:
        conn.close()


def list_released_batches():
    """供数目录：只列有效已放行批次。"""
    conn = store.connect()
    try:
        conn.execute("BEGIN")
        rows = conn.execute(
            "SELECT id, station_id, lower_limit, upper_limit, correction, status, "
            "invalid_reason, created_at, released_at, invalidated_at "
            "FROM qc_batches WHERE status = ? ORDER BY id",
            (RELEASED,),
        ).fetchall()
        out = [_batch_dict(conn, row) for row in rows]
        conn.commit()
        return out
    finally:
        conn.close()
