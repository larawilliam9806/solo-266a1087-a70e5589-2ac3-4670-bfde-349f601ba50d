# Click Lake Erasure API

面向广告数据分析的点击数据湖服务：按日追加上传点击 CSV、本地 Parquet 存储、按日下载，
并支持 GDPR 风格的用户数据清除（erasure）——受理后立即拦截该用户的新数据，后台重写
受影响文件，完成后永久拦截。上传中时间戳无法解析的坏行会被隔离保存，可事后提交正确
时间戳修复入湖。

技术栈：**FastAPI + PyArrow + SQLite**（SQLite 作为状态库，Parquet 作为数据湖格式）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，交互式文档见 `http://localhost:8000/docs`。
数据与状态持久化在命名卷 `clicklake-data` 中。

### 本地开发

```bash
pip install -r requirements-dev.txt
export DATA_DIR=./data STATE_DIR=./state
uvicorn --factory app.main:create_app --reload
```

### 运行测试

```bash
python -m pytest tests/ -q
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/upload/{date}` | 上传当日点击 CSV（multipart 文件字段 `file`），返回接收/拦截/入库/隔离行数与隔离记录 ID |
| `GET` | `/dates` | 列出所有日期（文件数、行数、是否受限） |
| `GET` | `/dates/{date}/download?format=csv\|parquet` | 按日下载；日期受限时返回 `409` |
| `GET` | `/quarantine` | 列出隔离坏行（可按 `user_id`、`status` 过滤） |
| `GET` | `/quarantine/{record_id}` | 查询单条隔离记录（原内容、上传日期、行号、状态） |
| `POST` | `/quarantine/{record_id}/repair` | 提交正确时间戳 `{"timestamp": "..."}` 修复该隔离行 |
| `POST` | `/erasure` | 提交清除请求 `{"user_id": "..."}`（幂等，重复提交返回同一任务） |
| `GET` | `/erasure/{task_id}` | 查询清除进度 |
| `GET` | `/erasure?user_id=...` | 列出清除任务 |
| `GET` | `/health` | 健康检查 |

### 上传 CSV 格式

表头必须包含 `user_id,page,timestamp`（额外列被忽略）。时间戳支持 ISO-8601
（建议带 `Z` 或时区偏移）与 Unix 秒，统一归一化为 UTC 存储。

```bash
curl -X POST -F "file=@clicks.csv" http://localhost:8000/upload/2026-11-01
# {"date":"2026-11-01","received":4,"invalid":1,"blocked":1,"stored":2,
#  "file_id":"...","quarantined":1,"quarantine_ids":["..."]}
```

- `received`：CSV 数据行总数；`invalid`：格式非法被跳过的行数（含被隔离的坏行）
- `blocked`：因清除请求被拦截（未写入）的行数；`stored`：实际入库行数
- `quarantined` / `quarantine_ids`：本次上传被隔离的坏行数及其记录 ID

### 坏行隔离与修复

`user_id` 有值但时间戳解析失败的行仍只计 `invalid` 并丢弃出有效数据流，但上传成功后
其**原内容、上传日期和行号**会作为隔离记录保存，响应返回可查询的记录 ID。其余非法行
（空 `user_id`、缺列等）按原规则跳过，不进入隔离。

```bash
curl http://localhost:8000/quarantine/<record_id>
# {"record_id":"...","date":"2026-11-01","line_no":2,"user_id":"alice","page":"/home",
#  "raw_timestamp":"not-a-time","raw_line":"alice,/home,not-a-time","status":"pending",...}

curl -X POST -H "Content-Type: application/json" \
     -d '{"timestamp":"2026-11-01T10:30:00Z"}' \
     http://localhost:8000/quarantine/<record_id>/repair
# {"status":"repaired","repaired_ts":"2026-11-01T10:30:00+00:00","file_id":"...",...}
```

修复把该点击写入**原上传日期**的分区；日期可下载（未受限）时即可看到。修复语义：

- **共同登记**：一次上传的有效行（文件记录）与隔离记录在同一事务中登记，失败不会
  留下孤立记录（半写的孤儿文件在重启恢复时清理）。
- **失败保留**：修复的写盘或登记失败时，隔离记录保持 `pending`，可直接重试。
- **不重复入湖**：修复在提交锁内"读记录 → 写 Parquet → 单事务登记"；已修复的记录
  重放只返回原结果。并发修复、响应丢失后的重试、重启后的重试都不会把点击写两次。
- **清除联动**：清除受理的同一事务内删除该用户的全部隔离记录；在途修复在锁内重读
  记录，不会把已清除用户的点击写回（已拦截用户的坏行在上传时也不会被隔离）；其他
  用户的隔离记录不受影响，仍可正常修复。修复入湖的点击登记了用户-日期索引，之后的
  清除任务会将其一并擦除。

### 清除流程

```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"user_id":"alice"}' http://localhost:8000/erasure
# 201 Created（重复提交返回 200 与同一 task_id）

curl http://localhost:8000/erasure/<task_id>
# {"status":"processing","files_total":2,"files_done":1,"restricted_dates":["2026-11-01"],...}
```

任务状态机：`pending → processing → completed`，失败时为 `failed`（自动重试，重启后继续）。

## 清除语义（保证）

- **立即拦截**：请求受理的同一事务内，该用户即进入拦截集合；之后提交的上传直接丢弃其记录。
- **在途上传不写回**：上传的"过滤 → 写 Parquet → 提交状态"与"受理清除"在同一把提交锁下
  完成，两种操作的临界区不会交错，因此在途上传不可能把被清除用户的数据写回。坏行修复的
  "读记录 → 写盘 → 登记"也在同一把锁下，在途修复同样写不回。
- **下载限制**：含该用户的日期在任务完成前暂停新下载（旧链接同样 `409`）；已开始的下载
  在日期锁保护下读取完数据后再响应，可正常结束；其他日期的导入与下载不受影响。
- **完成标准**：后台逐个重写受影响文件（tmp + fsync + 原子 rename），删除含目标数据的旧
  文件后才标记完成；其他用户记录完整保留。
- **交错安全**：单一后台 worker 顺序处理任务；文件被重写时，其他任务的待处理记录在
  同一事务内被重定向到新文件，多个清除与导入交错不会漏删、复活或丢失他人记录。
  完成前还有一次全量校验扫描作为兜底。
- **失败与重启**：写盘失败时任务进入 `failed` 并自动重试，期间拦截与日期限制保持有效；
  进程重启后恢复所有未完成任务，并清理临时文件、孤儿文件与未完成的删除。
- **幂等与永久拦截**：每个用户终生对应同一任务（`user_id` 唯一约束），重复提交返回同一
  任务；任务行永不删除，因此完成后该用户被永久拦截。

## 数据布局

```
data/
  date=2026-11-01/
    part-<file_id>.parquet      # 每次上传/修复追加一个 part 文件
state/
  erasure.db                    # SQLite：文件清单、任务、限制日期、用户-日期索引、隔离记录
```

Parquet schema：`user_id: string, page: string, ts: timestamp[us, tz=UTC]`。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | Parquet 数据目录 |
| `STATE_DIR` | `./state` | SQLite 状态目录 |
| `ERASURE_RETRY_DELAY` | `5` | 失败任务重试间隔（秒） |
| `ERASURE_WORKER_POLL` | `0.2` | 后台 worker 轮询间隔（秒） |
