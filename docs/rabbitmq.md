# RabbitMQ 分发与可靠性交接

## 职责与部署

PostgreSQL 保存任务状态、执行记录、调度计划和 Outbox；RabbitMQ 分发已经到期的执行通知。
任务处理器不直接依赖消息内容执行业务，而是通过数据库验证任务当前状态与执行凭证后领取。
Java 侧保留 Spring JdbcTemplate 和 TransactionTemplate，消息接入不改变现有事务正确性依据。

本地默认 `RABBIT_ENABLED=false`，无需 Broker。Compose 默认开启 RabbitMQ 模式，所有应用节点必须统一模式。
部署增加 `rabbitmq:4.2.9-management`，使用独立随机用户密码和持久数据卷，端口只在 Compose 内网开放。
该部署只有单个 Broker，classic 队列没有副本，不提供 Broker 高可用；PostgreSQL 高可用也仍由部署环境负责。

## 事务 Outbox

普通提交、指定时间任务、Cron 实例、失败重试和租约回收，均在改变任务状态的同一事务中创建事件。
事件携带 `eventId`、`taskId` 和 `expectedToken`，同一任务同一期望 token 有唯一约束。
token 在领取、取消和回收时递增；重试生成与当前 token 匹配的新执行通知，因此旧消息不能跨执行代次领取。
指定时间和重试退避保存在数据库中，Outbox 到期后才投递，无需另一套 MQ 延时插件或业务重试队列。

发送器先在数据库中取得有期限的发送租约，再在事务外发布，网络等待不会持续持有数据库行锁。
采用持久化消息、持久队列、Publisher Confirm 和 mandatory 路由退回处理。
仅确认成功且未被路由退回时标记发送完成；失败保留错误并退避重试，发送器宕机后租约到期可重新发送。
Broker 已确认、数据库标记前宕机可能导致重复发布，这是允许的至少一次通知投递。

默认每 30 秒运行低频修复，重新处理长期到期却未领取的任务；补偿有冷却时间并且不覆盖有效发送租约。
这覆盖升级前已有任务、Broker 消息丢失或异常通知被处理后的遗留任务；仍需监控持续补发和死信原因。
历史 Outbox 记录当前保留，不自动删除；长期部署应按终态任务保留周期安排归档，不能删除仍等待执行的代次。

## 消费与 ACK

消费者手动确认，预取量为 1。消费通知时，先取得本地执行容量，再提交数据库领取事务。
数据库校验 PENDING / RETRY、到期时间、执行次数和 expectedToken，成功后登记本地执行并 ACK。
ACK 表示该通知已可靠交接给数据库租约与本地执行管理，不代表业务完成。
若进程在 ACK 后、处理器开始前宕机，任务仍处于持有租约的 RUNNING，其他节点可在过期后回收。
若 ACK 丢失触发消息重投，数据库状态和 token 校验会拒绝再次领取，消费者可以确认过期通知。

预取限制只约束未确认消息。由于领取后即 ACK，本地执行容量仍须独立控制，满载时等待容量再领取。
旧执行忽略中断时继续占用槽位，直到线程真正退出，避免重试时覆盖槽位而多领任务。
正常业务失败由任务状态机写入 RETRY、退避时间及新的 Outbox 事件；不使用无限 nack / requeue 重试业务。
无法解析的通知进入死信队列，便于排查消息格式；临时通信失败与数据库失败不能直接当作业务永久失败。

ready 队列使用 classic 队列并声明 `x-max-priority=9`，消息映射现有 0..9 优先级。
优先级影响 Broker 等待消息的分发，不抢占已投递或执行的任务，不保证跨节点全局严格完成顺序。
持续高优先级任务仍可能使低优先级任务等待，当前没有优先级老化。

## 故障与验证

RabbitMQ 不可用时，API 仍可将任务与 Outbox 提交到数据库；聚合健康检查可能返回 DOWN，业务 API 无需等待 MQ。
Broker 恢复后，发送器重新投递未确认事件，消费者继续领取。运行中的任务仍可更新数据库结果。
数据库不可用时不能可靠提交或领取任务，不能仅凭 MQ 消息跳过数据库租约校验。

```sh
python scripts/rabbit_smoke.py
python scripts/rabbit_smoke.py --chaos
```

脚本首先检查 Worker 的 `taskforge_rabbit_received_total` 增量，确认任务经过 RabbitMQ 消费链路。
故障模式会停止 Broker 后提交一批任务，检查其保持 PENDING 且执行次数为 0，恢复后等待完成；
随后暂停两个执行节点，等 ready 队列形成积压，再重启 Broker，确认积压仍存在并恢复节点完成任务。
每个测试任务使用独立随机请求标识，finally 恢复停止的服务，不删除数据库或 RabbitMQ 数据卷。
原有 `smoke.py --chaos` 继续验证执行节点失联接管和数据库重启持久化。

新增 MQ 不直接证明吞吐提升。验证记录中的旧性能样本只适用于原版本，不能用来宣称本版本容量。
应结合发送失败次数、Outbox 积压、死信数、MQ 接收计数、任务调度延迟和数据库负载观察瓶颈。

参考：[RabbitMQ 确认机制](https://www.rabbitmq.com/docs/confirms)、
[预取控制](https://www.rabbitmq.com/docs/consumer-prefetch)、
[优先级队列](https://www.rabbitmq.com/docs/4.2/priority)。
