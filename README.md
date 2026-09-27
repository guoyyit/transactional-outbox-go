# Transactional Outbox for Go

把「业务数据变更」与「下游消息发布」收敛为一个本地事务：业务写入和待发布消息
**在同一个本地事务里原子提交/回滚**，消息随后由一条**独立投递链路（relay）**
可靠送达下游。进程崩溃、下游长期不可用、重复投递、清理与投递并发等场景都有明确
保证。

当前提供：

- `outbox`：核心抽象与投递链路（门面、Dispatcher、DLQ、Cleaner、配置）。
- `store/memory`：支持事务的进程内存储（测试、示例、单进程部署）。存储层通过
  `outbox.Store` 接口抽象，可替换为 SQL 等持久化实现。

---

## 1. 快速开始

```go
store := memory.New() // 或任何实现 outbox.Store 的持久化存储

publisher := outbox.PublisherFunc(func(ctx context.Context, m *outbox.Message) error {
    // 下游/边缘 broker 必须以稳定的 m.ID 做幂等去重
    return broker.Publish(m.Type, m.ID, m.Payload)
})

ob := outbox.New(store, publisher, outbox.WithConfig(outbox.DefaultConfig()))

// 业务写入 + 消息写入：同一本地事务，原子提交或一起回滚
err := ob.Run(ctx, func(tx outbox.Tx) error {
    if err := tx.Put(ctx, "orders/42", []byte("PAID")); err != nil {
        return err
    }
    _, err := tx.Enqueue(ctx, "orders", "order.paid",
        []byte(`{"orderId":42}`),
        outbox.WithID("idempotency-key-42"),
        outbox.WithMetadata(map[string]string{"traceId": "abc"}),
    )
    return err
})

// 独立投递链路：扫描已提交消息并送达
ob.Start(ctx)
defer ob.Stop()
```

完整可运行示例见 `outbox/example_test.go`。

---

## 2. 投递语义

**至少一次（at-least-once）投递 + 下游幂等去重 = 有效恰好一次。**

- 发件箱只承诺消息**不会丢失**、**顺序受控**，但在崩溃恢复、租约超时等场景，
  同一条消息的 `Publish` 可能被调用多次。
- 每条消息在 `Enqueue` 时分配一个**全局唯一、永不改变的 `Message.ID`**
  （可用 `outbox.WithID` 指定业务幂等键）。下游**必须**以该 ID 去重。
- 只有 `Publisher.Publish` 返回 `nil` 才认为消息被下游持久接受；返回 error 触发
  退避重试。

### 投递状态机

```
Pending ──claim──▶ InFlight ──ack成功──▶ Delivered（保留 Retention 后清理）
   ▲                  │
   │                  ├──失败且 Attempt<MaxAttempts──▶ Pending(AvailableAt=退避时刻)
   │                  │
   └──租约超时/崩溃恢复│
                      └──失败且 Attempt>=MaxAttempts──▶ Dead（独立死信集，可查询/重投）
Dead ──DLQ Replay──▶ Pending（Attempt 清零，按流内顺序重新参与投递）
```

---

## 3. 一致性保证（逐条对应需求）

### 3.1 业务写入与消息原子化

- `Outbox.Run` 在一个存储事务里执行业务 `Put/Delete` 与任意次 `Enqueue`。
- **提交**：业务数据与消息同时持久化；**回滚**：两者同时撤销。
- 事务提交前，消息对投递链路与其他读者**完全不可见**（见测试
  `TestNoIntermediateStateObserved`），因此任何中间状态都不可能被下游观察到。
- 背压准入（`MaxOutboxSize`）在事务内判断：超限则**整个事务（含业务写入）被
  拒绝**，不会留下半条业务数据。

### 3.2 崩溃恢复（不丢、已送达不重）

- Dispatcher 对消息的领取是一个**租约（lease）**：领取时置 `InFlight` 并写
  `LeaseUntil = now + LeaseDuration`。
- 工作进程在「事务提交后、消息送达前」任意时刻崩溃：消息仍在存储中。重启后新
  进程扫描到**租约已过期的 InFlight 消息**，将其恢复为 Pending 并重新投递
  （`TestCrashRecoveryAfterCommit`）。
- 为防止「慢工人」与「恢复工人」同时把结果写回，状态回写带**租约令牌**
  （= `LeaseUntil`）：旧工人迟到的成功确认会得到 `ErrLeaseLost` 而被丢弃，
  消息只可能被置为 Delivered 一次（`TestDuplicatePublishIsIdempotent`）。
  下游侧的重复 `Publish` 调用由下游用 `Message.ID` 幂等识别。

### 3.3 顺序与幂等

- 每条消息有流内单调递增的 `Seq`，在**提交时刻**按 `Enqueue` 顺序分配。
- 投递保证**同一 Stream（业务流）严格按提交顺序送达**：
  - 每次扫描对每个流**至多领取一条**消息；
  - 仅当队首到达终态（成功/重试/死信）后，后续消息才可被领取；
  - 队首处于退避、在途、死信时，**整个流在该消息之后阻塞**，绝不越过队首。
- 不同 Stream 之间相互独立、轮转领取，互不饿死（见 3.7）。
- 重试与租约恢复都不改变 `Seq`，因此不破坏顺序
  （`TestPerStreamOrdering`）。

### 3.4 退避重试与死信

- 失败按 `Backoff.Next(attempt)` 指数退避（默认 100ms 起步、翻倍、上限 30s），
  消息在 `AvailableAt` 之前不会被领取。
- 达到 `MaxAttempts` 后进入**独立死信集合**（`StatusDead`），记录最后一次错误。
- DLQ 支持按流查询、分页，以及单条 / 按流 / 全部 `Replay`。
- **重投不重复扣减业务副作用**：业务副作用在最初的业务事务里只执行一次；Replay
  只是把**同一条出站消息**重新排进投递队列，不会重新执行业务函数。重投仍然可能
  让下游收到重复 `Publish`，由 `Message.ID` 幂等处理（`TestDLQReplayRedelivers`）。
- 死信队首同样**阻塞该流后续消息**，保证重投后的顺序。

### 3.5 并发度、批次、在途上限与背压

| 机制 | 作用 |
| --- | --- |
| `Concurrency` | 并行 `Publish` 上限（工作信号量），避免打爆下游。 |
| `BatchSize` | 单次扫描最多领取的消息数。 |
| `MaxInFlight` | 全局「已领取未到终态」消息上限，约束 relay 占用的内存，与积压总量无关。 |
| `MaxOutboxSize` | 活动（未送达）消息的存储上限；下游长期不可用时，超限事务快速失败（`ErrOutboxFull`），发件箱不会无界增长、内存不会被耗尽。 |

- relay 循环**绝不因下游池满而阻塞扫描协程**：池满时新领取的消息在信号量上排队。
- 背压把压力传导给业务写入方（快速失败 + 调用方重试），而不是悄悄堆积。

### 3.6 已送达消息清理

- `Cleaner` 按 `CleanupInterval` 周期性删除「状态为 Delivered 且
  `UpdatedAt < now-Retention`」的消息，分批执行（默认每批 500）。
- **严格只删 Delivered**：Pending、InFlight（含租约已过期待恢复）、Dead 一律跳过
  （`TestCleanerSafety`），因此重试中与死信消息永远不会被清理漏掉。
- 删除是存储层的**原子条件操作**：在扫描与删除之间被领取的消息不会被误删，
  清理与投递并发执行不会产生悬挂引用或重复投递（`TestCleanerConcurrentWithDelivery`）。

### 3.7 多业务方独立业务流

- `Stream` 标识业务流；不同 Stream 的写入走各自的本地事务，领取时按流轮转，
  **互不阻塞**（一个流死信不影响其他流送达，见
  `TestIndependentStreamsAndStats`）。
- 每个流的重试进度、死信、已送达数、最后 Seq 可通过 `Store.StreamStats` 区分；
  DLQ 可按流查询/重放，全程可追溯。消息的 `Metadata` 可携带 tenant/trace 信息。

---

## 4. 全部配置项

| 配置 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `PollInterval` | duration | `200ms` | relay 扫描到期消息的间隔。 |
| `Concurrency` | int | `4` | 并行投递（`Publish`）最大并发。 |
| `BatchSize` | int | `32` | 每次扫描最多领取消息数。 |
| `MaxInFlight` | int | `64` | 全局在途（已领取未完结）消息上限，约束内存。 |
| `LeaseDuration` | duration | `30s` | 领取租约时长；超时即视为工作进程崩溃并可被恢复重投。应大于单次 `Publish` 的 P99 耗时。 |
| `MaxAttempts` | int | `8` | 最大投递尝试次数；超过后进入 DLQ。 |
| `Backoff` | Backoff | 指数退避 100ms→30s，×2 | 重试退避策略，可实现 `outbox.Backoff` 自定义。 |
| `Retention` | duration | `24h` | 已送达消息保留时长，超期才会被清理。 |
| `CleanupInterval` | duration | `1m` | 清理任务执行间隔。 |
| `MaxOutboxSize` | int | `100000` | 活动消息上限，超限事务返回 `ErrOutboxFull`，防止下游长期故障导致无界增长。 |

可选项（`outbox.New` 的 Option）：

- `WithConfig(Config)`：注入配置（零值字段会用默认值补齐）。
- `WithIDGenerator(IDGenerator)`：默认 128bit 随机 hex；测试可用 `SequenceIDGenerator`。
- `WithClock(Clock)`：注入时钟（测试用）。

`Enqueue` 选项：`WithID(id)`（业务幂等键）、`WithMetadata(map[string]string)`。

---

## 5. API 概览

- `outbox.New(store, publisher, opts...) *DefaultOutbox`
- `(*DefaultOutbox).Run(ctx, func(tx Tx) error)`：原子事务；`Tx` 提供
  `Enqueue / Put / Delete`。
- `(*DefaultOutbox).Start(ctx) / Stop()`：启停 relay 与清理器。
- `Dispatcher`：`RunOnce`（单轮，测试用）、`InFlight`、`WaitIdle`。
- `DLQ`：`List(stream, limit, offset)`、`Replay(id)`、`ReplayAll(stream)`。
- `Cleaner`：`PurgeOnce(ctx)`（也可只依赖后台周期清理）。
- `Store` 接口：`Transaction`、`ClaimDue`、`MarkDelivered/MarkRetry/MarkDead`
  （均带租约令牌）、`Replay/ReplayAll`、`DeleteDelivered`、`ListDead`、`Get`、
  `StreamStats`、`Size`。

---

## 6. 持久化实现注意事项（替换 memory 时）

实现 SQL/其他存储时需保持以下性质：

1. `Transaction` 必须是真实本地事务；`InsertMessage` 与业务 KV 写在同一事务，
   Seq 用流内原子自增（如事务内 `SELECT max(seq)+1 ... FOR UPDATE` 或专用序号表），
   在提交时确定。
2. `ClaimDue` 必须是**原子领取**（行锁 / `UPDATE ... WHERE status IN
   (pending,inflight_with_expired_lease) ... ORDER BY stream,seq LIMIT`，或
   `SKIP LOCKED` + 每流队首约束），并保证同一流同时只有一条在途。
3. `Mark*` 必须在 `WHERE` 条件里同时校验 `id` 与租约令牌（`lease_until`），
   过期/被恢复后旧工人写入影响行数为 0，即 `ErrLeaseLost`。
4. `DeleteDelivered` 必须带 `status = delivered AND updated_at < ?` 谓词原子删除。
5. 活动消息计数（`Size`/准入）与领取、删除在事务/行锁下一致，避免超发与竞态。

---

## 7. 测试

```
go test -race -count=1 ./...
```

覆盖（`outbox/*_test.go`）：

- **事务回滚 / 原子提交 / 中间状态不可见**：`TestAtomicCommit`、
  `TestAtomicRollback`、`TestNoIntermediateStateObserved`。
- **崩溃恢复**：`TestCrashRecoveryAfterCommit`、
  `TestCrashRecoveryMidDelivery`。
- **重复投递幂等**：`TestDuplicatePublishIsIdempotent`（租约超时后双投递、
  旧确认被拒）。
- **顺序投递（含失败重试）**：`TestPerStreamOrdering`、`TestDeliversCommittedMessages`。
- **重试与死信 / 重投不重复业务副作用**：`TestRetryThenDead`、
  `TestDLQReplayRedelivers`。
- **清理安全与并发**：`TestCleanerSafety`、`TestCleanerConcurrentWithDelivery`。
- **背压与有界增长**：`TestBackpressureRejectsWhenFull`。
- **多流互不阻塞与可追溯统计**：`TestIndependentStreamsAndStats`。
