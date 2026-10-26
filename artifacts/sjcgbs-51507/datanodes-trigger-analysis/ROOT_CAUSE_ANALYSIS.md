# SJCGBS-51507：DataNodes 首次更新失败的触发原因

后续PR评估已补查到IGNITE-27162 / #7813及其Raft客户端依赖，建议清单以`../pr-reassessment/PR_REASSESSMENT.md`为准。下文保留当轮根因证据与候选核查边界。

核查日期：2026-10-26。故障时间均为北京时间（UTC+8）。本轮只暂存旧功能、读取任务/日志、比对源码和官方变更，没有回移补丁或变更生产环境。

## 结论与证据边界

**目前最有证据支持的直接触发机制：node1 重启/成员替换期间，MetaStorage Leader node2 短暂失去多数派而退位；部分 MetaStorage 请求经历无 Leader/非 Leader响应后，等待 node1 超时并耗尽 10 秒重试预算；DataNodes 更新遇到此类异常后没有重新执行整个操作，异常传播至 Watch。**

这里必须区分三个证据层次：

1. **已直接证实**：node1 新实例启动；旧成员离开逻辑拓扑；node2 因多数派不足退位、随后重新当选；node0 多个 MetaStorage 读请求超时；同一拓扑事件对应 DataNodes 更新失败；随后 Watch 链失败。
2. **高可信机制判断，尚未逐条堆栈闭环**：DataNodes 本次失败由上述 MetaStorage 请求超时触发。日志将异常栈拆为独立记录，只有秒级采集时间，continuation 行无线程、trace、文件偏移；因此尚不能将某一条 TimeoutException 唯一关联到某个 DataNodes TraceId，不能把同窗的所有读超时都认定为 DataNodes 请求。
3. **仍未确定**：node1 为何重启；node2 为何短暂收不到 node0 的有效 Raft 响应（网络、调度/GC、资源争用等均需额外证据）；这些环境扰动是否与 hotfix 的其他改动有关。不能把多数派不足直接写成已证实网络分区、OOM 或 hotfix 缺陷。

## 本轮新增现场证据

| 时间 | 证据 | 含义 |
|---|---|---|
| 01:31:41.326 | node1 `IgniteImpl Starting node`，随后 main 线程输出 JVM、PID 和配置；新成员 UUID 为 `a911daf8-0189-4384-b3dd-b085ad0341e7` | 明确存在节点新实例启动，不能仅解释为 Primary 数量不均 |
| 01:31:45.891 | 旧 node1 从逻辑拓扑退出，版本146（历史采集） | 触发拓扑变更，DataNodes 处理的事件 HLC physical 为01:31:45.826 |
| 01:31:48.830–831 | node2 已成功处理多个 zone 的同一拓扑事件 | 当时并非所有 DataNodes 操作都失败 |
| **01:31:49.330** | node2：`steps down when alive nodes don't satisfy quorum`，term341，`deadNodes=node0,node1` | Leader 因近期有效响应不足而退位；deadNodes 是 Raft 视角，不等于两个进程都已死亡 |
| 01:31:49.331 | node2：`onLeaderStop ... Majority of the group dies: 2/3` | MetaStorage 多数派不足的直接证据 |
| **01:31:50.581** | node2：`become leader ... term=342`，此前 node0 授予 pre-vote | 重新选举很快完成，不能说 MetaStorage 一直无 Leader直到01:32:01 |
| 01:31:51.835 | term342 配置提交及 `onLeaderStart` | 元数据处理已恢复推进 |
| 01:31:53.362 | node0 DataNodes zone20 的同一拓扑事件处理成功 | 部分操作成功与其他未完成请求超时并存 |
| **01:32:00–01** | node0 三条 `Failed to update data nodes history and timers on topology change`，TraceId尾部 `75a1788a`、`8a0d174c`、`7778f740` | 最早已采集失败在01:32:00；不能继续只引用01:32:01这一条作为首条 |
| 01:32:01.148 | node0 Watch 对拓扑 keys 的通知失败（历史采集） | 更新失败升级为本地通知链失败 |
| 01:32:28.312 | 新 node1 进入逻辑拓扑147（历史采集） | 节点回来不等于失败的 Watch future 自动恢复 |

`node2-raft.json` 包含退位、重选和配置提交原始日志，`node1-before.json` 包含新实例启动日志，`first-error-metadata.json` 包含三条 DataNodes 外层异常。

同窗 `earliest-causes.json` 收到182条 Caused by记录，均为 `ReadActionRequestImpl` 的 `TimeoutException: Send with retry timed out`；返回记录可能包含重复栈，182不能解释为182个独立请求。典型重试原因：

- node2 返回 `EPERM: No leader at term 341`；另一些请求还经过 node0 的 `EPERM: Is not leader`（历史采集）。
- 接着访问 node1，单次等待约10.1–10.4秒后超时。
- `stopTime - startTime = 10000ms`，实际总耗时约11–12秒后报错。

**关键点：Leader已经恢复，正在等待其他 peer 的 RPC 仍可能尚未完成；Leader恢复不会自动让每条在途RPC立即重路由。** 这是“短暂选举问题”与“稍后某些操作仍超时”能够同时发生的解释，不能把它误判为持续10秒以上无Leader。

新查询没有返回 WriteActionRequestImpl、ClassCastException、JvmPauseMonitor 或 blocked 记录，但这些空结果未通过可用的负证据验证，**不能据此排除写请求失败、类型转换错误、JVM停顿或阻塞**。

## 对照源码：为什么更新会被超时击穿

以暂存后的 HEAD `65c9420c2ca771d70ff96301b4caa41c8f643c96` 为基线：

- `DataNodesManager.java:1027`：远程上下文读取走 `metaStorageManager.getAll(...)`。
- `DataNodesManager.java:1116`：首轮读本地；CAS返回false后递归重试，后续改为远程读。因此“更新DataNodes”可能最终失败在读阶段，而非写阶段。
- `DataNodesManager.java:1131–1151`：MAX_ATTEMPTS重试处理CAS冲突；transport/leader超时造成future异常时，没有重新执行完整操作，而是向 FailureProcessor 报告、让异常继续传播。递归读失败也会传播到外层invoke的whenComplete。
- `RaftGroupServiceImpl.java:638,671–695`：总预算以当前墙钟计算，在每次尝试前检查是否耗尽；单次RPC使用peer自己的响应超时，未按剩余总预算截断，所以10秒预算可能在11–12秒后才报错。这一日志现象本身不证明时钟跳变。
- `RaftGroupServiceImpl.java:738`起：超时和非Leader响应会触发客户端重试；但预算用尽后future仍会异常完成。
- `NodeImpl.java:2681–2795`：根据有效Raft响应判断多数派，失去多数派退位；这一判断和退位逻辑在本地hotfix中未修改。
- `WatchProcessor.java:238,436–442`：前一通知future异常后，串联的后续通知不会正常执行，并报告CRITICAL_ERROR。

`source-attribution.json` 记录了官方3.1.0与当前HEAD的blob比对：**DataNodesManager、RaftGroupServiceImpl、RetryContext三份文件完全相同**。因此上述“重试预算耗尽→更新异常→Watch失败”的机制继承自官方3.1.0，不是这些文件被hotfix改坏。该比对不能证明所有上游资源/通信扰动与hotfix无关，也不能替代生产JAR一致性校验。

## 官方 Jira / PR 的关联与适用范围

| 官方记录 | 实际问题/变更 | 对本次的判断 |
|---|---|---|
| [IGNITE-27801](https://issues.apache.org/jira/browse/IGNITE-27801) | 同样的DataNodes外层失败，cause为MetaStorage `MultiInvokeCommandImpl` 写请求重试超时；经历非Leader响应和peer超时 | **最接近的官方机制对照**，但现场目前捕获的是读请求超时，不能声称逐请求一致。官方以Cannot Reproduce关闭、无FixVersion，不存在可据此确认的根因补丁 |
| [IGNITE-26942](https://issues.apache.org/jira/browse/IGNITE-26942) / [PR #6903](https://github.com/apache/ignite-3/pull/6903) | 快照恢复幂等命令缓存后，将Boolean误当StatementResult，修复MultiInvoke返回类型转换 | 官方已合并；本地MetaStorageWriteHandler仍是旧逻辑。本次没有正证据显示命中ClassCastException，不能按同一外层DataNodes日志就套用此补丁 |
| [IGNITE-27436](https://issues.apache.org/jira/browse/IGNITE-27436) / [PR #7287](https://github.com/apache/ignite-3/pull/7287) | Raft客户端重试计时改用单调时钟 | 官方已合并，本地Raft客户端仍是旧逻辑。但现场耗时符合RPC超时/预算耗尽，未证明发生墙钟跳变；不能认定为本次根因修复 |
| [IGNITE-27157](https://issues.apache.org/jira/browse/IGNITE-27157) / [PR #7509](https://github.com/apache/ignite-3/pull/7509) | 为CMG服务引入可指定超时的Raft服务等改造 | IGNITE-27801关联此任务，但改动对象是CMG等服务；不能只凭关联关系宣称修复了DataNodes更新超时。本地未包含对应实现 |
| [IGNITE-21456](https://issues.apache.org/jira/browse/IGNITE-21456) / [PR #7563](https://github.com/apache/ignite-3/pull/7563) | 默认失败处理从noop改为stop | 本地未包含。处理异常节点继续在线的后果，不消除MetaStorage超时或短暂失去多数派 |
| [IGNITE-27324](https://issues.apache.org/jira/browse/IGNITE-27324) / [PR #7235](https://github.com/apache/ignite-3/pull/7235) | 独立线程停止节点，避免失败处理时同步停机死锁 | 本地未包含。与stop策略配套，不能当作DataNodes首次更新失败的修复 |

新检查的PR #6903、#7287、#7509元数据已通过GitHub连接核验，并以本地实际提交diff核对。Git祖先关系与blob比对见source-attribution.json；“非祖先”本身不等于没有回移，以上判断还结合了具体实现。

## 对Hotfix 6范围的含义

先区分两个验收目标：一是短暂MetaStorage不可用/选举后，DataNodes操作如何恢复并保证revision顺序与写入幂等；二是已不可恢复的Watch失败如何停止异常节点。#7563+#7235只覆盖后者。

后续定位最有价值的证据是：node1退出原因与Pod/进程重启记录；01:31:45–50 node0↔node2的Raft心跳/复制响应、JVM停顿和资源证据；未被拆散且带关联标识的DataNodes完整异常栈。没有这些证据，不宜宣称已证明网络故障、某个hotfix引入了故障，或某个官方PR能够根治本次问题。

## 日志与时间边界

本轮通过observability公开只读脚本，先发现索引，再查询 `prod / cbas / tic-ignite-cn-prod`，索引 `es_hxmonitor_L3:tic-ignite-log-*`，字段 `log_time`。每次query与结果均保存在同目录。

ES返回总数关系gte、部分查询被截断为1000条，分页请求失败，若干复杂查询超时；这些失败不属于“没有错误”的证据。正证据只取实际返回记录。

node1结果混有重启后重采集的10月25日旧日志，采集时间在10月26日01:31:44；不能将其当作本次故障时间线。表中关键带时间日志均核对message内原始时间，异常continuation则结合内部请求epoch和拓扑HLC分析，仍保留同秒关联限制。

## 旧功能暂存

- 本轮Primary观测/均衡旧改动：stash `b2decb97adf88c5b37ae85df2d6e2b05e8938fc5`，34文件逐一校验SHA256。
- 原Parquet stash `c63690e9bf0ff422219c1ebadf222648526ee10f`保持不变。
- 双份备份及恢复说明：`../primary-feature-shelved-20261026/RESTORE.md`。
- 暂存后tracked工作区与index均无diff；历史artifacts保留。此次仅新增调查证据与报告，没有修改运行源码。
