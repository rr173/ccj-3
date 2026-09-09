# stream-aligner — 双事件流水位对齐服务

两条来源不同、到达有早有晚的事件流（A / B），按**事件时间窗口 + 业务 key** 对齐成业务结果。
系统由三个可独立部署的服务组成：

```
                ┌──────────────┐   POST /events (幂等)
  事件源 A ───▶ │  ingest-a    │                │
                │  :8001       │◀───────────────┘
                │  独立水位     │   GET /events?after_seq=   (单调 seq 增量拉取)
                └──────┬───────┘   GET /watermark
                       │                    ▲
                       │ 拉取事件 + 水位      │
                ┌──────┴───────┐   ┌────────┴────────┐
                │   aligner    │   │    postgres     │
                │   :8003      │──▶│ stream_a/b、    │
                │  对齐 + 查询  │   │ aligner 三个库   │
                └──────┬───────┘   └─────────────────┘
                       │                    ▲
                ┌──────┴───────┐            │
                │  ingest-b    │────────────┘
                │  :8002       │   POST /events (幂等)
                └──────────────┘◀────────────── 事件源 B
```

- **ingest-a / ingest-b**：同一个镜像、不同环境变量的两次部署。各自持久化事件、
  按 `event_id` 幂等去重、维护**各自的水位**并对外暴露。
- **aligner**：周期性从两个 ingest 增量拉事件（offset 与写入同事务提交，崩溃重放不重复），
  仅当**两边水位都越过同一窗口末尾**才产出该窗口的对齐结果；晚到/回撤事件触发**订正**，
  每次变更落审计。所有查询由 aligner 提供。
- **postgres**：一个容器内三个逻辑库（`stream_a` / `stream_b` / `aligner`），服务间不共享表，
  各自可独立重建、迁移、部署。

## 语义定义

| 概念 | 定义 |
|---|---|
| 窗口 | 事件时间上的滚动窗口 `[ws, ws+WINDOW_SIZE_MS)`，`ws = floor(event_time / W) * W` |
| 流水位 | `max(event_time) - WATERMARK_GRACE_MS`；流空闲超过 `IDLE_TIMEOUT_MS` 后按墙钟推进；运维可用 override 强制（含回拨） |
| 关窗条件 | `min(wm_a, wm_b) >= window_end`。**只有一边越过不算**，两边都越过才产出 INITIAL 结果 |
| 对齐规则 | 窗口内同 key 的 A、B 事件按 `(event_time, event_id)` 排序后顺序配对；多余的一侧记入 `unmatched_*`；只有单边数据也出结果 |
| 晚到事件 | 落在已关窗窗口的事件 → 重算该 (窗口, key)，内容变化则产生新版本（`LATE_EVENT`） |
| 回撤 | `type=retract, retracts=<event_id>` 删除已收事件 → 重算并产生新版本（`RETRACTION`）；结果清空时版本状态为 `RETRACTED` |
| 订正幂等 | 重算结果内容哈希不变 → **不产生新版本**。同一结果绝不会被重复计算还当作成功 |

结果版本模型：每个 `(window_start, key)` 有一个 head 版本（`CURRENT` 或 `RETRACTED`），
旧版本置为 `SUPERSEDED` 但**永久保留**；每次版本跃迁写一条 `audit`（原因 + 触发事件明细），
`GET /results/history` 可以回答"某次结果为什么被改过"。

## 边界情况行为矩阵

| 情况 | 行为 |
|---|---|
| 重复到达 | ingest 按 `event_id` 唯一约束去重，响应里明确返回 `deduped`（不报错、不双算）；aligner 侧 `ON CONFLICT DO NOTHING` + offset 同事务提交，重放是 no-op |
| 窗口内乱序 | 配对按 `(event_time, event_id)` 排序，乱序不影响结果；重算结果与到达顺序无关（有单测保证） |
| 一边长时间无事件 | 空闲超过 `IDLE_TIMEOUT_MS` 后该流水位按墙钟推进，窗口照常关闭，单边数据产出带 `unmatched_*` 的结果；从未有过事件的流在启动超时后同样进入空闲推进，不会永久卡住另一边 |
| 水位被回拨 | override 可把水位调低；aligner 在 `watermark_log` 记录 `regress` 方向并告警，**已产出结果不被悄悄改写或删除**，此后到达的晚到数据仍走订正路径；清除 override 后恢复自动水位 |
| 回撤先于目标到达 | 回撤事件先存下；目标事件到达时即被过滤，不会短暂出现错误结果 |
| 服务重启 / 崩溃 | ingest 事件落库后才响应；aligner 的 offset 与事件写入同事务，崩溃最多重放未提交尾部且重放为 no-op；结果发版本由内容哈希决定，重启不会重复发版 |
| 空结果被清空 | 全部事件被回撤后产生 `RETRACTED` 状态版本（payload 为 null），之后再有数据可"复活"为新 `CURRENT` 版本 |

## 快速开始

```bash
cd stream-aligner
docker compose up -d --build     # 或 make up
bash scripts/demo.sh             # 或 make demo —— 完整演示下述所有场景
python3 tests/test_e2e.py        # 或 make test —— 对运行中的栈做端到端断言
python3 tests/test_core.py       # 或 make unit —— 纯逻辑单测，无需任何依赖
```

演示脚本依次验证：正常对齐 → 重复去重 → 双水位关窗 → 晚到订正（v2）→ 回撤订正（v3）→
历史/审计查询 → 空闲推进 → 水位回拨 + 回拨期间继续订正（v4）。

## API 一览

### ingest-a (:8001) / ingest-b (:8002) — 同一镜像

| 方法/路径 | 说明 |
|---|---|
| `POST /events` | 单个事件、数组或 `{"events":[...]}`；整批校验、单事务写入。响应 `{"accepted": n, "deduped": [...]}` |
| `GET /events?after_seq=&limit=` | 按单调 `seq` 增量拉取（aligner 的消费接口） |
| `GET /watermark` | 当前水位及来源（`event_time` / `idle_timeout` / `override` / `no_data`） |
| `POST /watermark/override` | `{"watermark": <ms>}` 强制水位（调低即回拨）；`{"watermark": null}` 清除 |
| `GET /healthz` | 健康检查 |

事件格式：

```json
{"event_id": "a1", "event_time": 1736000000000, "key": "order-1",
 "type": "upsert", "payload": {"amount": 100}}
{"event_id": "r1", "event_time": 1736000001000, "key": "order-1",
 "type": "retract", "retracts": "a1"}
```

### aligner (:8003)

| 方法/路径 | 说明 |
|---|---|
| `GET /results/current?window_start=&key=` | 当前 head 结果；不带参数返回全部 head |
| `GET /results/history?window_start=&key=` | 该结果的全部版本 + 审计记录（"为什么被改过"） |
| `GET /audit?window_start=&key=` | 全局审计流（INITIAL / LATE_EVENT / RETRACTION） |
| `GET /watermarks` | 两边当前水位与 `min_watermark` |
| `GET /watermarks/history?stream=` | 水位变更历史，含 `advance` / `regress` 方向 |
| `GET /windows` | 所有已知 (窗口, key) 的状态：是否关窗、head 版本、事件数 |
| `GET /healthz` | 健康检查 |

## 配置

| 环境变量 | 服务 | 默认 | 说明 |
|---|---|---|---|
| `STREAM_NAME` | ingest | — | 流名（`a` / `b`） |
| `DATABASE_DSN` | 全部 | — | 各自独立的库 |
| `WATERMARK_GRACE_MS` | ingest | 60000 | 水位宽限（允许乱序程度） |
| `IDLE_TIMEOUT_MS` | ingest | 300000 | 空闲多久后水位按墙钟推进 |
| `WINDOW_SIZE_MS` | aligner | 60000 | 滚动窗口大小 |
| `POLL_INTERVAL_MS` | aligner | 1000 | 拉取/关窗周期 |
| `PULL_BATCH_SIZE` | aligner | 500 | 单次拉取批量 |

compose 中演示配置为：窗口 30s、宽限 5s、空闲超时 20s，便于快速观察各种行为。

## 设计取舍与限制

- **拉模式 + 单调 seq**：aligner 周期性从 ingest 拉事件而非消息队列。少一个组件，
  语义等价（offset 即消费位点），重放天然幂等。要换 Kafka 只需替换 ingest 的存储层，
  对齐语义不变。
- **重算而非增量**：订正通过对 (窗口, key) 的全量重算 + 内容哈希比对实现，用计算换
  正确性——任何乱序/迟到/回撤组合都收敛到同一结果，且天然幂等。窗口内事件量极大时
  可再加增量聚合，但版本/审计语义不需要变。
- **单线程对齐循环**：一个窗口一个 key 的重算是 O(窗口内事件数)，串行足够演示与中小规模；
  水平扩展方向是按 key 哈希分片多个 aligner 实例（offset 表按分片拆分即可）。
- **水位回拨不撤销已发结果**：已对外给出的结果是既成事实，只能通过后续订正版本修正，
  不能悄悄抹掉——这正是审计存在的意义。
- 事件时间一律用 epoch 毫秒（BIGINT），水位与墙钟同域，空闲推进才有意义。
