"""质控放行领域逻辑单元测试（SQLite 本地副本，无需 PostgreSQL）。"""
import os
import tempfile
import threading
import unittest

from app import config, qc, store
from app.worker import ChangeConsumer, apply_change


class FakeCursor:
    def __init__(self):
        self.feedback = []

    def send_feedback(self, **kw):
        self.feedback.append(kw)


class FakeMsg:
    def __init__(self, payload, lsn):
        self.payload = payload
        self.data_start = lsn


class FakeWorker:
    def __init__(self):
        self.stop_event = threading.Event()


def seed_replica(stations, readings):
    conn = store.connect()
    conn.execute("DELETE FROM readings")
    conn.execute("DELETE FROM stations")
    for sid, corr in stations:
        conn.execute("INSERT INTO stations(id, correction) VALUES (?, ?)", (sid, corr))
    for rid, sid, val in readings:
        conn.execute(
            "INSERT INTO readings(id, station_id, value) VALUES (?, ?, ?)",
            (rid, sid, val),
        )
    conn.commit()
    conn.close()


def feed_txn(text, lsn, cursor=None, last=0):
    """走与生产相同的 ChangeConsumer 路径：失效+两表落库+LSN 单事务。"""
    consumer = ChangeConsumer(FakeWorker(), cursor or FakeCursor(), last)
    consumer(FakeMsg(text, lsn))
    return consumer


class QcTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.old_path = config.SQLITE_PATH
        config.SQLITE_PATH = os.path.join(self.tmp, "qc.db")
        store.init_db()
        store.set_meta("snapshot_done", "1")
        # station 1: correction 10；station 2: correction 0
        seed_replica(
            stations=[(1, 10), (2, 0)],
            readings=[
                (101, 1, 5),    # corrected 15
                (102, 1, -50),  # corrected -40
                (103, 1, 90),   # corrected 100
                (201, 2, 7),    # corrected 7
            ],
        )

    def tearDown(self):
        config.SQLITE_PATH = self.old_path

    # ---------------------------------------------------------- 建批与判定
    def test_create_batch_freezes_snapshot_and_flags_anomalies(self):
        b = qc.create_batch(1, lower_limit=0, upper_limit=50)
        self.assertEqual(b["status"], qc.VALID)
        self.assertEqual(b["correction"], 10)
        self.assertEqual(b["reading_count"], 3)
        # -40 与 100 超出 [0,50]；15 正常
        self.assertEqual(b["anomaly_count"], 2)
        detail = {r["reading_id"]: r for r in b["readings"]}
        self.assertEqual(detail[101]["corrected_value"], 15)
        self.assertFalse(detail[101]["is_anomaly"])
        self.assertTrue(detail[102]["is_anomaly"])
        self.assertTrue(detail[103]["is_anomaly"])

    def test_create_blocked_when_unpublished(self):
        store.set_meta("snapshot_done", "0")
        with self.assertRaises(qc.QcError):
            qc.create_batch(1, 0, 10)

    def test_create_unknown_station_and_bad_limits(self):
        with self.assertRaises(qc.QcError):
            qc.create_batch(999, 0, 10)
        with self.assertRaises(qc.QcError):
            qc.create_batch(1, 100, 0)

    def test_freeze_immune_to_later_source_changes(self):
        b = qc.create_batch(1, 0, 50)
        # 源端随后改了校正与读数：批内冻结内容不变（失效只改批次状态）。
        conn = store.connect()
        conn.execute("UPDATE stations SET correction = 99 WHERE id = 1")
        conn.execute("UPDATE readings SET value = 999 WHERE id = 101")
        conn.commit()
        conn.close()
        b2 = qc.get_batch(b["id"])
        self.assertEqual(b2["correction"], 10)
        self.assertEqual(
            {r["reading_id"]: r["value"] for r in b2["readings"]}[101], 5
        )

    # ---------------------------------------------------------- 确认与放行
    def test_must_confirm_every_anomaly_before_release(self):
        b = qc.create_batch(1, 0, 50)
        with self.assertRaises(qc.QcError):
            qc.release_batch(b["id"])
        qc.confirm_anomaly(b["id"], 102)
        with self.assertRaises(qc.QcError):  # 仍有 103 未确认
            qc.release_batch(b["id"])
        with self.assertRaises(qc.QcError):  # 正常读数不能确认
            qc.confirm_anomaly(b["id"], 101)
        qc.confirm_anomaly(b["id"], 103)
        released = qc.release_batch(b["id"])
        self.assertEqual(released["status"], qc.RELEASED)
        self.assertIsNotNone(released["released_at"])
        self.assertEqual(released["confirmed_count"], 2)

    def test_duplicate_confirm_rejected(self):
        b = qc.create_batch(1, 0, 50)
        qc.confirm_anomaly(b["id"], 102)
        with self.assertRaises(qc.QcError):
            qc.confirm_anomaly(b["id"], 102)

    def test_no_anomaly_batch_releases_directly(self):
        b = qc.create_batch(2, -100, 100)  # corrected 7 在区间内
        self.assertEqual(b["anomaly_count"], 0)
        self.assertEqual(qc.release_batch(b["id"])["status"], qc.RELEASED)

    def test_released_only_serves_valid_released_batches(self):
        b1 = qc.create_batch(1, 0, 50)
        with self.assertRaises(qc.QcError):  # 未放行不能供数
            qc.get_released_batch(b1["id"])
        qc.confirm_anomaly(b1["id"], 102)
        qc.confirm_anomaly(b1["id"], 103)
        qc.release_batch(b1["id"])
        served = qc.get_released_batch(b1["id"])
        self.assertEqual(served["id"], b1["id"])
        self.assertEqual(len(served["readings"]), 3)
        self.assertEqual([x["id"] for x in qc.list_released_batches()], [b1["id"]])

    # ---------------------------------------------------------- 失效规则
    def test_station_correction_change_invalidates(self):
        b = qc.create_batch(1, 0, 50)
        other = qc.create_batch(2, 0, 50)
        conn = store.connect()
        qc.invalidate_for_changes(
            conn, [("stations", "UPDATE", None, {"id": 1, "correction": 11})]
        )
        conn.commit()
        conn.close()
        self.assertEqual(qc.get_batch(b["id"])["status"], qc.INVALIDATED)
        self.assertIn(qc.REASON_STATION_CORRECTION,
                      qc.get_batch(b["id"])["invalid_reason"])
        # 无关站更正不影响
        self.assertEqual(qc.get_batch(other["id"])["status"], qc.VALID)

    def test_reading_change_invalidates_that_station_only(self):
        b1 = qc.create_batch(1, 0, 50)
        b2 = qc.create_batch(2, 0, 50)
        feed_txn(
            "BEGIN 1\n"
            "table public.readings: UPDATE: id[integer]:101 station_id[integer]:1 "
            "value[integer]:6\nCOMMIT 1",
            lsn=200,
        )
        self.assertEqual(qc.get_batch(b1["id"])["status"], qc.INVALIDATED)
        self.assertIn(qc.REASON_READING_CHANGED,
                      qc.get_batch(b1["id"])["invalid_reason"])
        self.assertEqual(qc.get_batch(b2["id"])["status"], qc.VALID)

    def test_move_affects_both_stations(self):
        b1 = qc.create_batch(1, 0, 50)
        b2 = qc.create_batch(2, 0, 50)
        # reading 101 从站点 1 迁到站点 2（改非键列，解码器只发新行）
        feed_txn(
            "BEGIN 1\n"
            "table public.readings: UPDATE: id[integer]:101 station_id[integer]:2 "
            "value[integer]:5\nCOMMIT 1",
            lsn=200,
        )
        for bid in (b1["id"], b2["id"]):
            d = qc.get_batch(bid)
            self.assertEqual(d["status"], qc.INVALIDATED)
            self.assertIn(qc.REASON_STATION_MOVED, d["invalid_reason"])

    def test_insert_and_delete_reading_invalidate_membership(self):
        b = qc.create_batch(1, 0, 50)
        feed_txn(
            "BEGIN 1\n"
            "table public.readings: INSERT: id[integer]:104 station_id[integer]:1 "
            "value[integer]:1\nCOMMIT 1",
            lsn=200,
        )
        self.assertEqual(qc.get_batch(b["id"])["status"], qc.INVALIDATED)

        b3 = qc.create_batch(1, 0, 50)
        feed_txn(
            "BEGIN 2\ntable public.readings: DELETE: id[integer]:104\nCOMMIT 2",
            lsn=210, last=200,
        )
        self.assertEqual(qc.get_batch(b3["id"])["status"], qc.INVALIDATED)

    def test_released_batch_also_invalidated_and_never_served_again(self):
        b = qc.create_batch(2, -100, 100)
        qc.release_batch(b["id"])
        feed_txn(
            "BEGIN 1\n"
            "table public.readings: UPDATE: id[integer]:201 station_id[integer]:2 "
            "value[integer]:8\nCOMMIT 1",
            lsn=200,
        )
        d = qc.get_batch(b["id"])
        self.assertEqual(d["status"], qc.INVALIDATED)
        with self.assertRaises(qc.QcError):
            qc.get_released_batch(b["id"])
        self.assertEqual(qc.list_released_batches(), [])
        # 永久失效：改回去也不恢复
        feed_txn(
            "BEGIN 2\n"
            "table public.readings: UPDATE: id[integer]:201 station_id[integer]:2 "
            "value[integer]:7\nCOMMIT 2",
            lsn=210, last=200,
        )
        self.assertEqual(qc.get_batch(b["id"])["status"], qc.INVALIDATED)

    def test_stale_batch_cannot_be_released_after_interleaved_correction(self):
        """审核与更正交错：确认进行中源端已提交变更，放行必须被拒绝。"""
        b = qc.create_batch(1, 0, 50)
        qc.confirm_anomaly(b["id"], 102)
        feed_txn(
            "BEGIN 1\n"
            "table public.stations: UPDATE: id[integer]:1 correction[integer]:11\n"
            "COMMIT 1",
            lsn=200,
        )
        self.assertEqual(qc.get_batch(b["id"])["status"], qc.INVALIDATED)
        with self.assertRaises(qc.QcError):
            qc.confirm_anomaly(b["id"], 103)
        with self.assertRaises(qc.QcError):
            qc.release_batch(b["id"])

    def test_invalidation_lands_in_same_txn_as_two_table_changes(self):
        """失效与两表落库同一事务：模拟提交失败回滚，两边都不留痕。"""
        b = qc.create_batch(1, 0, 50)
        conn = store.connect()
        conn.execute("BEGIN")
        qc.invalidate_for_changes(
            conn, [
                ("stations", "UPDATE", None, {"id": 1, "correction": 77}),
                ("readings", "UPDATE", None,
                 {"id": 101, "station_id": 1, "value": 77}),
            ]
        )
        apply_change(conn, ("stations", "UPDATE", None,
                            {"id": 1, "correction": 77}))
        apply_change(conn, ("readings", "UPDATE", None,
                            {"id": 101, "station_id": 1, "value": 77}))
        conn.rollback()  # 写入失败
        conn.close()
        d = qc.get_batch(b["id"])
        self.assertEqual(d["status"], qc.VALID)  # 无半份失效
        conn = store.connect()
        self.assertEqual(
            conn.execute("SELECT correction FROM stations WHERE id=1").fetchone()[0], 10
        )
        self.assertEqual(
            conn.execute("SELECT value FROM readings WHERE id=101").fetchone()[0], 5
        )
        conn.close()

    # ---------------------------------------------------------- 重置与重启
    def test_reset_invalidates_all_but_keeps_history(self):
        b1 = qc.create_batch(1, 0, 50)
        qc.confirm_anomaly(b1["id"], 102)
        b2 = qc.create_batch(2, -100, 100)
        qc.release_batch(b2["id"])
        # b1 先因校正变更失效；随后副本重置：原原因与失效时间保留并追加重置原因
        conn = store.connect()
        qc.invalidate_for_changes(
            conn, [("stations", "UPDATE", None, {"id": 1, "correction": 11})]
        )
        conn.commit()
        conn.close()
        first_invalidated_at = qc.get_batch(b1["id"])["invalidated_at"]
        store.clear_replica()  # 生产路径：reset
        for bid in (b1["id"], b2["id"]):
            d = qc.get_batch(bid)
            self.assertEqual(d["status"], qc.INVALIDATED)
            self.assertIn(qc.REASON_REPLICA_RESET, d["invalid_reason"])
            # 当时输入与确认仍保留
            self.assertGreaterEqual(d["reading_count"], 1)
        d1 = qc.get_batch(b1["id"])
        self.assertIn(qc.REASON_STATION_CORRECTION, d1["invalid_reason"])
        self.assertEqual(d1["invalidated_at"], first_invalidated_at)
        self.assertEqual(d1["confirmed_count"], 1)
        # 重置后副本未发布，不能建批
        with self.assertRaises(qc.QcError):
            qc.create_batch(1, 0, 50)
        # 重新发布后可新建批次，旧批依旧失效
        store.set_meta("snapshot_done", "1")
        seed_replica([(1, 10)], [(101, 1, 5)])
        b3 = qc.create_batch(1, 0, 50)
        self.assertEqual(b3["status"], qc.VALID)
        self.assertEqual(qc.get_batch(b1["id"])["status"], qc.INVALIDATED)

    def test_conclusions_persist_across_reopen(self):
        """重启等价：结论全部在 SQLite，重开连接即可恢复。"""
        b = qc.create_batch(1, 0, 50)
        qc.confirm_anomaly(b["id"], 102)
        qc.confirm_anomaly(b["id"], 103)
        qc.release_batch(b["id"])
        # 新连接（模拟进程重启）
        self.assertEqual(
            store.connect().execute(
                "SELECT status FROM qc_batches WHERE id=?", (b["id"],)
            ).fetchone()[0],
            qc.RELEASED,
        )
        self.assertEqual(
            qc.get_released_batch(b["id"])["confirmed_count"], 2
        )

    def test_snapshot_rebuild_invalidates_with_distinct_reason(self):
        b = qc.create_batch(1, 0, 50)
        conn = store.connect()
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM stations")
        conn.execute("DELETE FROM readings")
        qc.invalidate_all_on_reset(conn, qc.REASON_SNAPSHOT_REBUILD)
        conn.commit()
        conn.close()
        d = qc.get_batch(b["id"])
        self.assertEqual(d["status"], qc.INVALIDATED)
        self.assertIn(qc.REASON_SNAPSHOT_REBUILD, d["invalid_reason"])

    # ---------------------------------------------------------- 同事务跨表
    def test_cross_table_txn_invalidates_atomically_via_consumer(self):
        b1 = qc.create_batch(1, 0, 50)
        b2 = qc.create_batch(2, 0, 50)
        cur = FakeCursor()
        feed_txn(
            "BEGIN 1\n"
            "table public.stations: UPDATE: id[integer]:1 correction[integer]:21\n"
            "table public.readings: UPDATE: id[integer]:201 station_id[integer]:2 "
            "value[integer]:70\nCOMMIT 1",
            lsn=300, cursor=cur,
        )
        # 两站批次都失效，且 LSN 同事务落库
        self.assertEqual(qc.get_batch(b1["id"])["status"], qc.INVALIDATED)
        self.assertEqual(qc.get_batch(b2["id"])["status"], qc.INVALIDATED)
        self.assertEqual(store.get_meta("last_applied_lsn"), "300")
        self.assertEqual(cur.feedback[-1]["flush_lsn"], 300)


if __name__ == "__main__":
    unittest.main()
