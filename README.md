# TaskForge

一个使用 Java 17、Spring Boot 3.5、MyBatis-Flex、PostgreSQL 和 RabbitMQ 实现的分布式任务调度与执行平台。
业务提交报表计算、内容摘要等任务，平台负责延时执行、周期触发、并发控制、重试和执行记录。
多节点部署通过事务 Outbox 将到期任务发送到 RabbitMQ，执行节点经数据库校验后领取，
失联任务在租约过期后可由其他节点接管。本地开发保留无需 RabbitMQ 的数据库轮询模式。

调度核心独立实现，没有接入现成调度框架。设计参考 [PowerJob](https://github.com/PowerJob/PowerJob)
和 [db-scheduler](https://github.com/kagkarlsson/db-scheduler)，未复制其实现代码。

## 已实现

- 立即任务、指定时间任务、六字段 Cron 周期任务、IANA 时区。
- 幂等提交；相同请求标识配不同参数返回 409。
- 事务 Outbox、RabbitMQ 持久化分发、发布确认与路由退回处理、手动 ACK 和异常消息死信队列。
- 多执行节点竞争领取、数据库时间租约、心跳续租、故障接管、过期执行结果隔离。
- 有界执行线程池，按空闲执行容量领取任务，优先级 0..9。
- 有限重试、指数退避、执行超时、协作式取消。
- 任务进度、每次执行记录、周期任务实例查询、集群节点心跳。
- 管理控制台、API Key 鉴权、请求体限制、健康检查、Prometheus 指标。
- H2 本地模式、PostgreSQL 多节点部署、Flyway 迁移、Docker Compose、CI。
- PostgreSQL 并发测试、容器故障验收脚本、可复现的压测脚本。

## 本地开发

要求 Java 17 和 Maven 3.9+。完整测试还需要正在运行的 Docker。

```sh
mvn verify
mvn spring-boot:run
```

打开 <http://127.0.0.1:8080>，开发模式密钥为 `local-development-key`。
默认 H2 文件库保存在 `data/`；重启保留任务。H2 文件模式只用于单进程开发。
本地默认 `RABBIT_ENABLED=false`，执行节点直接轮询数据库；Docker Compose 默认开启 RabbitMQ。
非回环地址监听时，必须设置独立的 `TASKFORGE_API_KEY`，长度至少 16 字符。

没有 Docker 时可以执行 H2、API 和执行器测试：

```sh
mvn test -Dtest='!PostgresTest,!RabbitIntegrationTest'
```

## 多节点部署

先构建 JAR，再生成仅保存于本机的部署配置。Python 3 只用于辅助脚本。

```sh
mvn verify
python scripts/setup.py
docker compose up -d --build --wait
docker compose ps
```

打开 <http://127.0.0.1:18080>。在项目根目录 `.env` 中查看 `TASKFORGE_API_KEY` 后填入页面。
`.env` 包含随机生成的应用密钥、数据库密码和 RabbitMQ 密码，已加入 Git 忽略规则；不要提交或分享。
已有 `.env` 再次执行 setup 只追加缺失配置，不替换已有值。RabbitMQ 已初始化后修改密码还需同步更新 Broker 用户。

部署包含 `api`、`worker-a`、`worker-b`、`db`、`rabbitmq` 五个服务。两个执行节点各有两个执行槽位。
API 节点不执行任务；执行节点不向宿主机暴露端口。控制台可查看所有节点的状态。
所有应用节点必须使用相同数据库、Broker 和分发模式，各节点 `NODE_ID` 应保持唯一。
RabbitMQ 只在 Compose 内网开放 AMQP 和管理接口，持久化数据保存在独立卷。

```sh
python scripts/smoke.py
python scripts/smoke.py --chaos
python scripts/rabbit_smoke.py --chaos
python scripts/benchmark.py --count 100 --concurrency 8
docker compose logs --tail 100 worker-a worker-b
docker compose down
```

`--chaos` 会强制终止本项目的一个执行节点并重启本项目数据库，测试后恢复执行节点。
`rabbit_smoke.py --chaos` 会停止和重启本项目的 Broker 与执行节点，验证 Outbox 补发和积压消息持久化；
脚本在 finally 中恢复服务，不删除数据卷。
请在演示环境运行。`docker compose down` 保留数据库卷；再次启动仍可查询历史任务。
不要在需要保留数据时使用 `down -v`。

## API 示例

所有 `/api/` 接口和 `/actuator/prometheus` 需要 `X-API-Key` 请求头。
`GET /actuator/health` 不需要鉴权。默认没有跨域授权，管理页面和 API 同源。

```http
POST /api/tasks
Content-Type: application/json
X-API-Key: <your-key>

{
  "requestKey": "report-2026-10-16-001",
  "handler": "report",
  "payload": {"values": [10, 20, 30]},
  "priority": 0,
  "maxAttempts": 3,
  "retryDelaySeconds": 2,
  "timeoutSeconds": 60
}
```

返回 201 和任务对象。任务完成后 `result` 包含 `count`、`sum`、`average`。
需要指定时间时增加 `availableAt`，格式为带偏移量的 ISO 8601 时间，例如 `2026-12-01T09:00:00+08:00`。
网络重试必须复用相同的 `requestKey` 和参数。请求标识在当前部署中全局唯一。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/tasks` | 提交任务 |
| GET | `/api/tasks?status=RUNNING&limit=50&offset=0` | 筛选与分页 |
| GET | `/api/tasks/{id}` | 任务状态、结果与进度 |
| POST | `/api/tasks/{id}/cancel` | 取消非终态任务；对终态任务无副作用 |
| GET | `/api/tasks/{id}/attempts` | 每次执行的节点、状态、耗时依据与错误 |
| GET | `/api/overview` | 任务数量、处理器与节点状态 |
| POST / GET | `/api/schedules` | 创建 / 查询周期任务 |
| GET | `/api/schedules/{id}` | 周期任务详情 |
| PUT | `/api/schedules/{id}/enabled` | 请求体 `{"enabled":false}` 暂停 |
| GET | `/api/schedules/{id}/tasks` | 周期任务产生的执行实例 |

列表 `limit` 范围 1..200，`offset` 范围 0..100000。错误响应包含 `status`、`message`。
请求体最大 64 KiB，任务参数最多 32768 字符，执行结果最多 65536 字符。

周期任务创建示例：

```json
{
  "name": "每分钟统计",
  "cron": "0 * * * * *",
  "zone": "Asia/Shanghai",
  "task": {
    "handler": "report",
    "payload": {"values": [10, 20, 30]},
    "priority": 0,
    "maxAttempts": 3,
    "retryDelaySeconds": 2,
    "timeoutSeconds": 60
  }
}
```

## 扩展业务处理器

实现 `TaskHandler` 并注册为 Spring Bean，提交任务时使用其 `name()`。
`validate` 校验参数；`execute` 执行业务，通过 `TaskContext.reportProgress` 更新进度。
长任务需定期调用 `checkCancelled()`，并让阻塞调用支持超时或中断。

内置处理器：`echo` 回显、`sha256` 文本摘要、`report` 数值统计、`delay` 延时演示。
`examples/TaskForgeClient.java` 是无需第三方依赖的 Java 17 接入示例。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DB_URL` | 本地 H2 文件库 | 多节点使用相同 PostgreSQL JDBC URL |
| `DB_USER` / `DB_PASSWORD` | `sa` / 空 | 数据库账号 |
| `TASKFORGE_API_KEY` | 本地开发密钥 | 所有管理 API 的共享密钥 |
| `BIND_ADDRESS` / `PORT` | `127.0.0.1` / `8080` | 监听地址与端口 |
| `NODE_ID` | 随机 UUID | 单节点标识 |
| `WORKER_ENABLED` | `true` | 是否领取任务、触发周期任务和接管失联任务 |
| `WORKER_CONCURRENCY` | `4` | 执行槽位数，范围 1..128 |
| `LEASE_SECONDS` | `30` | 租约时长，范围 6..3600 秒 |
| `POLL_MS` | `500` | 领取轮询间隔，范围 50..60000 毫秒 |
| `RABBIT_ENABLED` | `false` | 使用 Outbox / RabbitMQ 分发；Compose 为 true |
| `RABBIT_HOST` / `RABBIT_PORT` | `localhost` / `5672` | RabbitMQ AMQP 地址 |
| `RABBIT_USER` / `RABBIT_PASSWORD` | `guest` / `guest` | 本地连接默认值；Compose 使用独立随机密码 |

心跳与故障扫描每秒执行一次，节点状态每两秒更新。
指标包括 `taskforge_worker_active`、`taskforge_worker_capacity`、`taskforge_task_claimed_total`、
`taskforge_task_recovered_total` 和按 outcome 分类的执行耗时。
指标按进程统计；采集整个集群时需分别抓取各节点。
RabbitMQ 接收计数 `taskforge_rabbit_received_total` 按 Worker 进程统计。

## 语义与边界

平台提供至少一次执行语义。数据库中的执行凭证可以拒绝旧结果写回，不能撤销业务系统中已发生的副作用。
RabbitMQ ACK 表示通知已交接给数据库租约和本地执行管理，不表示业务已经成功；之后的宕机由租约扫描恢复。
涉及扣款、发邮件等外部操作时，处理器必须按业务标识实现幂等；任务超时和取消不代表外部副作用已撤销。
取消采用协作中断，无法强制终止忽略中断的 Java 代码，这类代码仍占用本节点执行槽位。

Cron 采用跳过积压时刻的策略：停机恢复后，对已到期调度生成一个实例，再推进到当前时间之后。
同一周期任务的不同实例可能并发执行。暂停调度不会取消已经生成的任务。
重复设置相同启停状态不会改变计划；恢复暂停的调度时跳过暂停期间的触发时刻。
高优先级持续到达时，低优先级任务可能等待；当前没有抢占或优先级老化机制。
RabbitMQ 模式使用 0..9 的 classic 优先级队列，已投递或执行中的任务不抢占，不承诺全局严格执行顺序。

本版本使用共享 API Key，适合受控环境；尚未实现多租户权限、任意脚本执行或 DAG 工作流。
历史任务和执行记录保留在数据库中，尚未自动清理；长期部署需按业务保留周期规划归档。
详细设计与验证记录见 [架构说明](docs/architecture.md) 和 [验证记录](docs/validation.md)。
消息分发、故障恢复和运维说明见 [RabbitMQ 设计](docs/rabbitmq.md)。单节点 Broker 和 classic 队列不提供副本高可用。
数据访问、实体映射与事务缓存配置见 [MyBatis-Flex 说明](docs/persistence.md)。

## PR 规则

每个 PR 的新增行数与删除行数之和不得超过 500；二进制变更会被检查脚本拒绝。
CI 对所有文件计数，无生成文件或文档豁免，然后执行 `mvn verify`，包括 PostgreSQL 容器测试。

```sh
python scripts/check_pr_size.py origin/main HEAD
```

项目使用 MIT 许可证。
