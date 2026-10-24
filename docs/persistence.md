# MyBatis-Flex 数据访问

数据访问使用 MyBatis-Flex 1.11.8 的 Spring Boot 3 starter；数据库结构继续由 Flyway 管理。
迁移不修改已有表、历史任务、执行凭证或接口 JSON，已有 PostgreSQL 数据可以继续使用。

## 分工

`persistence` 包中的 `@Table` 实体描述数据库行，`BaseMapper` 负责普通插入和查询，
`QueryWrapper` 负责筛选、排序和分页。接口仍返回 Task、Attempt、TaskSchedule 等 record，
持久化对象不会直接暴露到 API。周期任务的 JSON 配置在服务边界显式转换。

领取、续租、接管、Outbox 预留和补偿使用 Mapper 中的参数化 SQL。
这些更新必须保留状态、owner、token、到期时间等全部条件，并检查受影响行数。
SQL 中的 NULL 显式清除旧 owner、租约、错误和结果，不依赖 ORM 忽略空值的更新策略。
所有请求值通过 `#{...}` 或 QueryWrapper 绑定，排序列为程序固定值。

## 事务与时钟

Mapper 和 TransactionTemplate 使用同一 DataSource，由 Spring 管理连接、提交和回滚。
提交任务采用 NESTED 保存点：重复请求触发唯一约束时，回滚保存点后仍能在外层事务查询原任务。
任务插入、重试与失联恢复均在更新任务的同一事务中写入 Outbox；Cron 推进与实例创建也共享事务。
Outbox 发送租约在网络发送前提交，不在数据库事务内等待 RabbitMQ 确认。

MyBatis 配置 `local-cache-scope: STATEMENT` 与 `cache-enabled: false`。
同一事务的再次查询必须读取数据库，不能缓存任务状态、行锁查询或 `clock_timestamp()`。
PostgreSQL 租约校验继续在取得行锁后查询实际数据库时钟；不能用事务起始时间代替。
H2 嵌入式模式与应用共享时钟，多节点部署仍使用 PostgreSQL。

Instant 使用 MyBatis 时间类型处理器，写入前截断到微秒，保持原有数据库精度。
选择性插入保留 created_at、enabled 等数据库默认值，业务生成的 UUID 使用显式主键插入。

## 验证

实体映射、默认值、微秒精度、事务回滚和同事务重读由 MapperPersistenceTest 覆盖。
原有重复提交、竞争领取、租约锁等待、取消、Cron、Outbox 和 RabbitMQ 测试继续验证业务行为。
测试中的 JdbcTemplate 仅用于独立核对数据库和构造锁等待等故障，生产数据访问全部经过 Mapper。

参考：[MyBatis-Flex 快速开始](https://mybatis-flex.com/zh/intro/getting-started.html)、
[原生 MyBatis 用法](https://mybatis-flex.com/zh/intro/use-mybatis-native.html)、
[MyBatis 缓存配置](https://mybatis.org/mybatis-3/configuration.html)、
[Spring 事务集成](https://mybatis.org/spring/transactions.html)。
