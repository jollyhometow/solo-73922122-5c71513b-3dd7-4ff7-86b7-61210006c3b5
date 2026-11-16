# 离线质控数据副本采集器

监测数据离线质控：分析侧只读**本地副本**，不直连采集库。
FastAPI + SQLite 采集网页，从本地 PostgreSQL 同步 `stations(id, correction)` 与
`readings(id, station_id, value)`（各至多 500 行、非空整数、源端仅行级增改删）。
另在副本上提供**质控放行页**（`/qc`）：数据员把核查过的站点读数整批冻结、
逐条确认异常后放行给离线分析。

## 功能

- 网页可**启动同步**、**查看进度**（全量进度条 / 流式状态 / 已应用事务数）、**查询副本**
- 首次**全量同步**期间源库仍可写；随后经**逻辑解码**（`test_decoding`）持续接收已提交更正
- 全量 → 增量**衔接不漏收、不重复生效**（复制槽一致点导出快照）
- 同一源事务对两表的修改**一起可见**（单 SQLite 事务落库）
- 断连 / 采集进程退出重启后**从已持久化位置接续**：不跳过未落库事务、不回退新值
- 全量中断**自动重建**；**未完成不开放副本**（查询返回 409）
- 源离线时**已发布副本仍可查**并显示断连
- **仅一个采集进程**，严格保持源事务提交顺序
- **质控放行**（`/qc`）：选站 + 整数上下限建批，冻结该站当前全部 readings 与
  stations.correction，按 `value + correction` 判异常；逐条确认全部异常后才能放行；
  供数只返回有效已放行整批；源端已提交变化使相关批次**永久失效**（含迁站两站、
  副本重置全部旧批），历史输入/确认/失效原因永久保留

## 架构

```
PostgreSQL (wal_level=logical)
  │  ① CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')   一致点 + 导出快照 → 全量拷贝
  │  ② START_REPLICATION SLOT ... (test_decoding)        持续接收已提交事务
  ▼
collector（FastAPI 单进程，单个后台同步线程）
  │  每个源事务 = 一个 SQLite 事务（同事务写入 last_applied_lsn + 质控批次失效标记）
  ▼
SQLite 副本（WAL）  ⇠  网页 / API 只读查询、质控放行（qc_batches 等三张表）
```

### 正确性要点

| 要求 | 实现 |
| --- | --- |
| 衔接不漏不重 | 建槽时 `SNAPSHOT 'export'`：快照恰好对应槽的一致点。点之前已提交的进快照，点之后已提交的进流，二者无缝拼接 |
| 不跳过未落库事务 | 源事务的变更与 `last_applied_lsn` 在**同一个 SQLite 事务**提交；只有落库后才向源端发送 feedback 推进 `confirmed_flush_lsn` |
| 不重复生效 / 不回退新值 | 崩溃重放的提交若 `commit_lsn ≤ 已持久化 last_applied_lsn` 则跳过；且 upsert/delete 幂等；严格按源提交顺序单消费者应用 |
| 跨表原子可见 | BEGIN..COMMIT 间的两表变更先缓冲，COMMIT 时**一个 SQLite 事务**落库；读者（WAL）只见完整源事务 |
| 断连恢复 | 连接断开自动重连；槽位置服务端持久化，本地 LSN 持久化于 SQLite meta |
| 进程重启 | `sync_requested` 标记持久化；重启后自动续传：快照未完成则重建，已完成则从持久化 LSN 继续 |
| 全量中断可重建 | 快照整体一个 SQLite 事务，中断即回滚，副本保持未发布；重启后丢弃旧槽重建 |
| 单采集进程 | 进程内启动锁 + 源端槽占用检测（槽被占用时拒绝第二个采集者并提示） |

### 质控放行正确性要点

| 要求 | 实现 |
| --- | --- |
| 冻结整批 | 建批在单个 SQLite 事务内把该站当前全部 readings 行与 correction 复制到 `qc_batch_readings`，异常按冻结的 `value + correction` 与整数上下限判定；源端之后任何变化都不改变批内内容 |
| 逐条确认后才放行 | 放行 SQL 以 `NOT EXISTS` 检查确认表，存在未确认异常即 409；重复确认/确认非异常均拒绝；无异常批次可直接放行 |
| 失效与两表落库同事务 | 源事务落库前在**同一 SQLite 事务**内调用 `qc.invalidate_for_changes`：先回查变更前旧归属再写新值，提交后读者只能同时看到“新数据 + 批次已失效”，不存在数据已变而批次仍有效的窗口 |
| 审核与更正交错不能放行过期批 | 放行用条件 `UPDATE ... WHERE status='valid'` 兜底；失效后确认与放行一律 409，且失效永久（之后改回原值不恢复） |
| 失效范围 | 校正变更→该站；读数 value/成员增删→该站；读数迁站（station_id 变化，解码器不发旧非键列，落库前回查旧归属）→**新旧两站**；无关站更正不影响其他站批次 |
| 供数只给有效整批 | `/api/qc/released` 仅返回 `released` 且未失效批次；状态与明细在同一个读事务（`BEGIN`）内取出，必然对应同一已提交快照 |
| 副本重置 / 全量重建 | 两表被整体替换时，在同一事务内把全部批次标记失效（重置原因或“全量重建”原因），不删除质控历史：已失效批次追加原因，冻结输入、确认记录、原失效时间全部保留 |
| 重启保留结论 / 无半份写入 | 批次、确认、失效全部为 SQLite 已提交数据，重启即恢复；所有写操作单事务 `BEGIN IMMEDIATE`，异常即回滚，不留半份审核或放行 |
| 未发布不能建批 | 建批先检查 `snapshot_done=1`，否则 409 |

## 运行（Docker Compose）

```bash
docker compose up -d --build
# 浏览器打开 http://localhost:8000 ，点击「开始同步」
```

- `db`：postgres:16（`wal_level=logical`），首次启动由 `db/init.sql` 建表并写入
  200 个 stations + 500 条 readings
- `collector`：采集网页（FastAPI），SQLite 存于命名卷 `replica-data`（重启后续传）

### API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页（启动同步 / 进度 / 副本查询） |
| POST | `/api/sync/start` | 启动同步（幂等；已运行则提示） |
| POST | `/api/sync/reset` | 停止采集、清空副本与同步状态、删除复制槽 |
| GET | `/api/status` | 阶段 / 连接状态 / 进度 / 已应用事务数 / 已持久化 LSN |
| GET | `/api/stations` | 查询 stations 副本（未发布返回 409） |
| GET | `/api/readings?station_id=&limit=` | 查询 readings 副本（未发布返回 409） |
| GET | `/api/replica/dump` | 两表**同一读事务**的一致快照（用于跨表一致性校验） |
| GET | `/qc` | 质控放行网页（建批 / 逐条确认 / 放行 / 供数目录） |
| POST | `/api/qc/batches` | 建批：`{station_id, lower_limit, upper_limit}`（均整数；未发布 409） |
| GET | `/api/qc/batches` | 全部批次概览（含已失效历史） |
| GET | `/api/qc/batches/{id}` | 批次状态 + 全部冻结明细（同一读事务快照） |
| POST | `/api/qc/batches/{id}/confirm` | 逐条确认异常：`{reading_id}`（非异常/重复/失效均 409） |
| POST | `/api/qc/batches/{id}/release` | 放行（仍有未确认异常或已失效 → 409） |
| GET | `/api/qc/released` | 供数目录：仅有效已放行批次 |
| GET | `/api/qc/released/{id}` | 供数整批：有效已放行批次完整冻结快照，否则 409 |

### 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | `postgresql://postgres:postgres@localhost:5432/appdb` | 源库 DSN |
| `SQLITE_PATH` | `./replica.db` | 副本文件路径 |
| `SLOT_NAME` | `collector_slot` | 逻辑复制槽名 |
| `SNAPSHOT_ROW_DELAY_MS` | `0` | 全量拷贝每行人为延迟（演示用；compose 中设为 20 以便演示“全量中变更”） |
| `RECONNECT_DELAY_S` | `2` | 断连重试间隔 |
| `STATUS_INTERVAL_S` | `2` | 流式反馈间隔 |

## 演示脚本

```bash
docker compose up -d --build
python3 demo/demo.py        # 仅需 Python 标准库 + docker
```

依次演示并自动校验：

1. **全量中跨表更正**：全量期间副本 409 未开放；同一事务更正 `stations`+`readings`，
   全量结束后一致生效，且副本与源完全一致
2. **跨表原子可见**：流式期间连续 10 次跨表更正，经 `/api/replica/dump`（单读事务）
   轮询，断言从不出现半提交状态
3. **断连**：`docker compose stop db` 后状态显示断连、已发布副本仍可查询；
   `start db` 后自动恢复，新写入继续同步
4. **采集进程重启**：`docker compose restart collector` 后从持久化位置接续，
   重启前已提交事务无漏收、无重复生效（全量比对一致）
5. **全量中重启**：全量进行中重启采集进程，副本保持未开放，自动重建后最终一致

## 测试

```bash
cd collector && python3 -m unittest discover -s tests
# test_decoder/test_consumer：解码器与源事务应用；test_qc：质控失效/确认/放行/重置；
# test_api：质控 HTTP 层（409 语义与完整放行供数流程）
```

## 限制（按数据契约）

- 源库需 PostgreSQL ≥ 15（`CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export')` 语法）；
  compose 使用 postgres:16
- 仅支持两表固定 schema 的行级增改删；不支持 DDL / TRUNCATE
- 使用内置 `test_decoding` 文本输出解析；列值按契约均为非空整数
- 副本只读；`collector` 服务不可 `--scale` 扩容（单采集进程保证）
- 源库硬断连后，重连占用中的复制槽可能需等待源端 `wal_sender_timeout`（默认 60s）释放
