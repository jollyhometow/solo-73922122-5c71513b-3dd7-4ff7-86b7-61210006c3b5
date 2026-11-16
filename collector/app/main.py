"""FastAPI web app: start sync, watch progress, query the local replica."""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from . import config, qc, store
from .worker import STATUS, WORKER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("collector.main")


@asynccontextmanager
async def lifespan(_app):
    store.init_db()
    # Resume automatically after a process restart if sync was ever started:
    # an unfinished snapshot is rebuilt, a finished one resumes streaming from
    # the persisted LSN.
    if store.get_meta("sync_requested") == "1":
        log.info("resuming sync from persisted state")
        WORKER.start()
    yield
    WORKER.stop()


app = FastAPI(title="Offline QC replica collector", lifespan=lifespan)


@app.get("/api/status")
def api_status():
    st = STATUS.snapshot()
    st.update(
        published=store.published(),
        sync_requested=store.get_meta("sync_requested") == "1",
        last_applied_lsn=store.get_meta("last_applied_lsn"),
        slot_name=config.SLOT_NAME,
        worker_running=WORKER.running(),
        snapshot_row_delay_ms=config.SNAPSHOT_ROW_DELAY_MS,
    )
    return st


@app.post("/api/sync/start")
def api_sync_start():
    store.set_meta("sync_requested", "1")
    ok, msg = WORKER.start()
    return {"ok": ok, "message": msg}


@app.post("/api/sync/reset")
def api_sync_reset():
    try:
        WORKER.reset_local()
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "message": "replica cleared; start sync to rebuild"}


def _require_published():
    if not store.published():
        raise HTTPException(
            status_code=409,
            detail="replica not published yet (initial snapshot incomplete)",
        )


@app.get("/api/stations")
def api_stations():
    _require_published()
    rows = store.fetch_stations()
    return {
        "count": len(rows),
        "rows": [{"id": r[0], "correction": r[1]} for r in rows],
    }


@app.get("/api/readings")
def api_readings(
    station_id: int | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
):
    _require_published()
    rows = store.fetch_readings(station_id, limit)
    return {
        "count": len(rows),
        "rows": [
            {"id": r[0], "station_id": r[1], "value": r[2]} for r in rows
        ],
    }


@app.get("/api/replica/dump")
def api_replica_dump():
    """Both tables in one read transaction (consistent cross-table view)."""
    _require_published()
    stations, readings = store.fetch_dump()
    return {
        "stations": [{"id": r[0], "correction": r[1]} for r in stations],
        "readings": [
            {"id": r[0], "station_id": r[1], "value": r[2]} for r in readings
        ],
    }


# ------------------------------------------------------------ 质控放行 API
class BatchCreateRequest(BaseModel):
    station_id: int
    lower_limit: int
    upper_limit: int


class ConfirmRequest(BaseModel):
    reading_id: int


def _qc_http_error(exc):
    raise HTTPException(status_code=409, detail=str(exc))


@app.post("/api/qc/batches", status_code=201)
def api_qc_create_batch(req: BatchCreateRequest):
    """选站 + 整数上下限建批：冻结该站全部 readings 与 correction。"""
    try:
        return qc.create_batch(req.station_id, req.lower_limit, req.upper_limit)
    except qc.QcError as exc:
        _qc_http_error(exc)


@app.get("/api/qc/batches")
def api_qc_list_batches():
    """全部批次概览（含已失效历史）。"""
    return {"rows": qc.list_batches()}


@app.get("/api/qc/batches/{batch_id}")
def api_qc_get_batch(batch_id: int):
    """批次状态与全部冻结明细，取自同一已提交快照。"""
    try:
        return qc.get_batch(batch_id)
    except qc.QcError as exc:
        _qc_http_error(exc)


@app.post("/api/qc/batches/{batch_id}/confirm")
def api_qc_confirm(batch_id: int, req: ConfirmRequest):
    """逐条确认一条异常读数。"""
    try:
        return qc.confirm_anomaly(batch_id, req.reading_id)
    except qc.QcError as exc:
        _qc_http_error(exc)


@app.post("/api/qc/batches/{batch_id}/release")
def api_qc_release(batch_id: int):
    """全部异常确认后才能放行；失效批次永远拒绝。"""
    try:
        return qc.release_batch(batch_id)
    except qc.QcError as exc:
        _qc_http_error(exc)


@app.get("/api/qc/released")
def api_qc_released_list():
    """供数目录：只返回有效已放行整批。"""
    return {"rows": qc.list_released_batches()}


@app.get("/api/qc/released/{batch_id}")
def api_qc_released_detail(batch_id: int):
    """供数：有效已放行批次的完整冻结快照；失效/未放行一律 409。"""
    try:
        return qc.get_released_batch(batch_id)
    except qc.QcError as exc:
        _qc_http_error(exc)


@app.get("/qc", response_class=HTMLResponse)
def qc_page():
    return QC_HTML_PAGE


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_PAGE


HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>离线质控数据副本采集</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  h1 { font-size: 20px; }
  .card { border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; color: #fff; font-size: 13px; }
  .idle { background: #888; } .snapshot { background: #d9a000; }
  .streaming { background: #1a7f37; } .disconnected { background: #b02a37; }
  .error { background: #6f1d1b; }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
  .on { background: #1a7f37; } .off { background: #b02a37; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #ddd; padding: 4px 8px; text-align: left; }
  th { background: #f5f5f5; }
  button { padding: 6px 14px; margin-right: 8px; border-radius: 6px; border: 1px solid #999;
           background: #fff; cursor: pointer; }
  button:hover { background: #eee; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  .kv { font-family: ui-monospace, monospace; font-size: 13px; }
  .kv b { display: inline-block; width: 150px; font-weight: 600; }
  .bar { height: 14px; background: #eee; border-radius: 7px; overflow: hidden; margin-top: 6px; }
  .bar > div { height: 100%; background: #d9a000; width: 0%; transition: width .3s; }
  .warn { color: #b02a37; font-weight: 600; }
  .muted { color: #777; font-size: 12px; }
</style>
</head>
<body>
<h1>离线质控数据副本采集器
  <span class="muted" style="font-size:13px; font-weight:400">
    ｜ <a href="/qc" style="color:#1a7f37">质控放行页 →</a>
  </span>
</h1>

<div class="card">
  <div>
    <span class="dot" id="connDot"></span><span id="connText">…</span>
    &nbsp;&nbsp;阶段: <span class="badge idle" id="phase">…</span>
    &nbsp;&nbsp;<span id="msg" class="muted"></span>
  </div>
  <div class="bar" id="barWrap" style="display:none"><div id="bar"></div></div>
  <div class="kv" style="margin-top:10px">
    <div><b>副本已发布</b><span id="published">…</span></div>
    <div><b>全量进度</b><span id="prog">…</span></div>
    <div><b>已应用事务/变更</b><span id="applied">…</span></div>
    <div><b>已持久化 LSN</b><span id="lsn">…</span></div>
    <div><b>最近源事务时间</b><span id="lastCommit">…</span></div>
    <div><b>复制槽</b><span id="slot">…</span></div>
  </div>
  <div style="margin-top:12px">
    <button onclick="startSync()">开始同步</button>
    <button onclick="resetSync()">重置并清空副本</button>
    <span class="muted" id="hint"></span>
  </div>
</div>

<div class="card" id="closedNote" style="display:none">
  <span class="warn">副本未发布：初始全量同步未完成，查询已关闭。</span>
</div>
<div class="card" id="staleNote" style="display:none">
  <span class="warn">源库断连：以下为最后一次成功同步的已发布副本。</span>
</div>

<div class="grid">
  <div class="card">
    <b>stations 副本</b> <span class="muted" id="stCount"></span>
    <div style="max-height:300px; overflow:auto; margin-top:8px">
      <table><thead><tr><th>id</th><th>correction</th></tr></thead>
      <tbody id="stationsBody"></tbody></table>
    </div>
  </div>
  <div class="card">
    <b>readings 副本</b> <span class="muted" id="rdCount"></span>
    <div style="max-height:300px; overflow:auto; margin-top:8px">
      <table><thead><tr><th>id</th><th>station_id</th><th>value</th></tr></thead>
      <tbody id="readingsBody"></tbody></table>
    </div>
  </div>
</div>

<script>
async function j(url, method) {
  const r = await fetch(url, {method: method || 'GET'});
  if (!r.ok) throw new Error(url + ' -> ' + r.status);
  return r.json();
}
async function poll() {
  try {
    const s = await j('/api/status');
    const ph = document.getElementById('phase');
    ph.textContent = s.phase; ph.className = 'badge ' + s.phase;
    document.getElementById('connDot').className = 'dot ' + (s.connected ? 'on' : 'off');
    document.getElementById('connText').textContent = s.connected ? '源库已连接' : '源库未连接';
    document.getElementById('msg').textContent = s.message || '';
    document.getElementById('published').textContent = s.published ? '是' : '否';
    document.getElementById('prog').textContent =
      s.phase === 'snapshot' ? (s.rows_copied + ' / ' + s.total_rows + ' 行')
                             : (s.published ? '完成' : '未开始');
    document.getElementById('applied').textContent = s.applied_txns + ' / ' + s.applied_changes;
    document.getElementById('lsn').textContent = s.last_applied_lsn || '…';
    document.getElementById('lastCommit').textContent = s.last_commit_at || '…';
    document.getElementById('slot').textContent = s.slot_name;
    document.getElementById('barWrap').style.display = s.phase === 'snapshot' ? 'block' : 'none';
    if (s.total_rows > 0)
      document.getElementById('bar').style.width = (100 * s.rows_copied / s.total_rows) + '%';
    document.getElementById('closedNote').style.display = s.published ? 'none' : 'block';
    document.getElementById('staleNote').style.display = (!s.connected && s.published) ? 'block' : 'none';
    document.getElementById('hint').textContent =
      s.worker_running ? '' : (s.sync_requested ? '工作线程未运行' : '尚未启动同步');
  } catch (e) { /* ignore transient errors */ }
}
async function loadTables() {
  let s;
  try { s = await j('/api/status'); } catch (e) { return; }
  if (!s.published) return;
  try {
    const st = await j('/api/stations');
    document.getElementById('stCount').textContent = '(' + st.count + ' 行)';
    document.getElementById('stationsBody').innerHTML =
      st.rows.map(r => `<tr><td>${r.id}</td><td>${r.correction}</td></tr>`).join('');
    const rd = await j('/api/readings?limit=1000');
    document.getElementById('rdCount').textContent = '(' + rd.count + ' 行)';
    document.getElementById('readingsBody').innerHTML =
      rd.rows.map(r => `<tr><td>${r.id}</td><td>${r.station_id}</td><td>${r.value}</td></tr>`).join('');
  } catch (e) { /* replica closed mid-refresh */ }
}
async function startSync() { await j('/api/sync/start', 'POST'); poll(); }
async function resetSync() {
  if (confirm('确定要清空本地副本与同步状态并重新开始吗？')) { await j('/api/sync/reset', 'POST'); poll(); }
}
setInterval(poll, 1000); setInterval(loadTables, 2000);
poll(); loadTables();
</script>
</body>
</html>
"""


QC_HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>质控放行</title>
<style>
  body { font-family: system-ui, sans-serif; margin: 24px; color: #222; }
  h1 { font-size: 20px; }
  h2 { font-size: 16px; margin: 0 0 10px; }
  .card { border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; color: #fff; font-size: 12px; }
  .valid { background: #d9a000; } .released { background: #1a7f37; }
  .invalidated { background: #b02a37; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; }
  th, td { border: 1px solid #ddd; padding: 4px 8px; text-align: left; }
  th { background: #f5f5f5; }
  button { padding: 6px 14px; border-radius: 6px; border: 1px solid #999;
           background: #fff; cursor: pointer; }
  button:hover { background: #eee; }
  button:disabled { color: #aaa; cursor: not-allowed; }
  input, select { padding: 5px 8px; border: 1px solid #999; border-radius: 6px; font-size: 13px; }
  .muted { color: #777; font-size: 12px; }
  .warn { color: #b02a37; font-weight: 600; }
  .anom { background: #fdecea; }
  .ok { color: #1a7f37; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  #msg { margin-top: 8px; font-size: 13px; }
  .err { color: #b02a37; } .success { color: #1a7f37; }
  .scroll { max-height: 360px; overflow: auto; }
</style>
</head>
<body>
<h1>质控放行 <span class="muted" style="font-size:13px; font-weight:400">
  ｜ <a href="/" style="color:#1a7f37">← 返回采集页</a></span></h1>

<div class="card" id="closedNote" style="display:none">
  <span class="warn">副本未发布：初始全量同步未完成或正在重置重建，不能建批。</span>
</div>

<div class="card">
  <h2>新建质控批</h2>
  <form id="createForm" onsubmit="return createBatch(event)">
    站点
    <select id="stationSelect" required></select>
    下限（整数）<input id="lower" type="number" step="1" required style="width:110px">
    上限（整数）<input id="upper" type="number" step="1" required style="width:110px">
    <button type="submit">冻结并建批</button>
  </form>
  <div class="muted" style="margin-top:8px">
    建批即冻结该站点当前全部 readings 与 stations.correction；以 value+correction
    超出上下限判异常，逐条确认全部异常后才能放行。
  </div>
  <div id="msg"></div>
</div>

<div class="grid">
  <div class="card">
    <h2>批次列表（含历史）</h2>
    <div class="scroll">
      <table>
        <thead><tr><th>ID</th><th>站点</th><th>上下限</th><th>冻结校正</th>
        <th>读数/异常/已确认</th><th>状态</th><th>失效原因</th><th></th></tr></thead>
        <tbody id="batchBody"></tbody>
      </table>
    </div>
  </div>
  <div class="card">
    <h2 id="detailTitle">批次明细</h2>
    <div id="detail" class="muted">点击左侧「查看」加载批次明细。</div>
  </div>
</div>

<div class="card">
  <h2>供数（离线分析只取有效已放行整批）</h2>
  <div class="scroll">
    <table>
      <thead><tr><th>批次ID</th><th>站点</th><th>放行时间</th><th>读数条数</th><th></th></tr></thead>
      <tbody id="releasedBody"></tbody>
    </table>
  </div>
</div>

<script>
let selectedBatch = null;

function flash(text, ok) {
  const el = document.getElementById('msg');
  el.className = ok ? 'success' : 'err';
  el.textContent = text;
  if (ok) setTimeout(() => { el.textContent = ''; }, 4000);
}

async function api(url, options) {
  const r = await fetch(url, options || {});
  let body = {};
  try { body = await r.json(); } catch (e) {}
  if (!r.ok) throw new Error(body.detail || (url + ' -> ' + r.status));
  return body;
}

async function refreshStations() {
  let s;
  try { s = await api('/api/status'); } catch (e) { return; }
  document.getElementById('closedNote').style.display = s.published ? 'none' : 'block';
  document.getElementById('createForm').style.opacity = s.published ? '1' : '0.5';
  if (!s.published) return;
  const st = await api('/api/stations');
  const sel = document.getElementById('stationSelect');
  const cur = sel.value;
  sel.innerHTML = st.rows.map(r =>
    `<option value="${r.id}">${r.id}（correction=${r.correction}）</option>`).join('');
  if (cur) sel.value = cur;
}

async function createBatch(ev) {
  ev.preventDefault();
  const payload = {
    station_id: parseInt(document.getElementById('stationSelect').value, 10),
    lower_limit: parseInt(document.getElementById('lower').value, 10),
    upper_limit: parseInt(document.getElementById('upper').value, 10),
  };
  try {
    const b = await api('/api/qc/batches', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    flash('已建批 #' + b.id + '，冻结 ' + b.reading_count + ' 条读数，异常 '
          + b.anomaly_count + ' 条', true);
    selectedBatch = b.id;
    await refreshAll();
  } catch (e) { flash(e.message, false); }
  return false;
}

function statusBadge(st) {
  const label = {valid: '在途', released: '已放行', invalidated: '已失效'}[st] || st;
  return `<span class="badge ${st}">${label}</span>`;
}

async function refreshBatches() {
  const rows = (await api('/api/qc/batches')).rows;
  document.getElementById('batchBody').innerHTML = rows.map(b => `
    <tr>
      <td>${b.id}</td><td>${b.station_id}</td>
      <td>[${b.lower_limit}, ${b.upper_limit}]</td>
      <td>${b.correction}</td>
      <td>${b.reading_count} / ${b.anomaly_count} / ${b.confirmed_count}</td>
      <td>${statusBadge(b.status)}</td>
      <td class="warn" style="font-weight:400">${b.invalid_reason || ''}</td>
      <td><button onclick="viewBatch(${b.id})">查看</button></td>
    </tr>`).join('') || '<tr><td colspan="8" class="muted">暂无批次</td></tr>';
}

async function viewBatch(id) { selectedBatch = id; await loadDetail(); }

async function loadDetail() {
  if (selectedBatch == null) return;
  let b;
  try { b = await api('/api/qc/batches/' + selectedBatch); }
  catch (e) { document.getElementById('detail').innerHTML =
      '<span class="err">' + e.message + '</span>'; return; }
  document.getElementById('detailTitle').textContent =
    '批次 #' + b.id + ' 明细（站点 ' + b.station_id + '，冻结 correction='
    + b.correction + '，判定区间 [' + b.lower_limit + ', ' + b.upper_limit + ']）';
  const allConfirmed = b.anomaly_count > 0 && b.confirmed_count >= b.anomaly_count;
  const noAnom = b.anomaly_count === 0;
  const canRelease = b.status === 'valid' && (allConfirmed || noAnom);
  const rows = b.readings.map(r => `
    <tr class="${r.is_anomaly ? 'anom' : ''}">
      <td>${r.reading_id}</td><td>${r.station_id}</td><td>${r.value}</td>
      <td>${r.corrected_value}</td>
      <td>${r.is_anomaly ? '<b>异常</b>' : '正常'}</td>
      <td>${r.is_anomaly ? (r.confirmed
          ? '<span class="ok">已确认 ' + r.confirmed_at + '</span>'
          : '<button onclick="confirmOne(' + b.id + ',' + r.reading_id + ')">确认异常</button>')
        : '—'}</td>
    </tr>`).join('');
  document.getElementById('detail').innerHTML = `
    <div style="margin-bottom:8px">
      状态：${statusBadge(b.status)}
      ${b.invalid_reason ? ' <span class="warn">失效原因：' + b.invalid_reason
          + '（' + (b.invalidated_at || '') + '）</span>' : ''}
      ${b.released_at ? ' <span class="muted">放行时间：' + b.released_at + '</span>' : ''}
    </div>
    <div style="margin-bottom:8px">
      异常 ${b.anomaly_count} 条，已确认 ${b.confirmed_count} 条。
      <button ${canRelease ? '' : 'disabled'} onclick="releaseBatch(${b.id})">放行批次</button>
      ${b.status === 'valid' && !canRelease ? '<span class="muted">需逐条确认全部异常后方可放行</span>' : ''}
      ${b.status !== 'valid' ? '<span class="muted">该批次不可再操作</span>' : ''}
    </div>
    <div class="scroll"><table>
      <thead><tr><th>reading_id</th><th>station_id</th><th>冻结 value</th>
      <th>value+correction</th><th>判定</th><th>确认</th></tr></thead>
      <tbody>${rows}</tbody></table></div>`;
}

async function confirmOne(bid, rid) {
  try {
    await api('/api/qc/batches/' + bid + '/confirm', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({reading_id: rid}),
    });
    await refreshBatches(); await loadDetail();
  } catch (e) { flash(e.message, false); }
}

async function releaseBatch(bid) {
  try {
    await api('/api/qc/batches/' + bid + '/release', {method: 'POST'});
    flash('批次 #' + bid + ' 已放行', true);
    await refreshAll();
  } catch (e) { flash(e.message, false); }
}

async function refreshReleased() {
  const rows = (await api('/api/qc/released')).rows;
  document.getElementById('releasedBody').innerHTML = rows.map(b => `
    <tr><td>${b.id}</td><td>${b.station_id}</td><td>${b.released_at}</td>
    <td>${b.reading_count}</td>
    <td><a href="/api/qc/released/${b.id}" target="_blank">
      <button type="button">取整批 JSON</button></a></td></tr>`).join('')
    || '<tr><td colspan="5" class="muted">暂无有效已放行批次</td></tr>';
}

async function refreshAll() {
  try {
    await refreshStations();
    await refreshBatches();
    await refreshReleased();
    await loadDetail();
  } catch (e) { /* transient */ }
}
setInterval(refreshAll, 2000);
refreshAll();
</script>
</body>
</html>
"""
