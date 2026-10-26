# SJCGBS-51507 / Hotfix 6：官方 PR 重新评估

> 后续实施状态（2026-10-26）：用户已授权研发，11 项修复语义已完成 3.1 适配并验证。677 项通过、2 项既有禁用跳过；三节点网络恢复、选主、致命 Watch 停机重启及公式用例通过。本文“没有实施”的表述对应先前评估阶段，实施结果与边界以 [实施与验证报告](../hotfix6-implementation/IMPLEMENTATION_AND_VERIFICATION.md) 为准。

补充交叉核查：本地GridGain 9.1.28已包含核心恢复链、异步默认stop及重试/事务配套的关键等效实现；原始JAR组件探针也确认Watch失败后不提供原地恢复。详见`../gridgain-watch-comparison-20261026/GRIDGAIN_WATCH_FIX_ASSESSMENT.md`。该探针不替代Hotfix 6或GridGain真实集群故障验收。

日期：2026-10-26。基线：`65c9420c2ca771d70ff96301b4caa41c8f643c96`。旧Primary功能及Parquet改动保持暂存。本次为源码/官方记录评估，没有实施回移，没有执行故障注入或生产变更。

## 最终引入决策：官方PR与GridGain 9.1.28交叉核查

**最终建议纳入11项PR的完整修复语义，包括#7509的CMG客户端迁移。** 复核后将此前“#7509只提取依赖”的范围建议修正为完整语义回移：没有发现必须拆分的技术障碍，而Hotfix 6已经计划适配该PR所需的新Raft客户端。回移代码以Apache官方提交为来源；GridGain发布包用于核对实际接线、修复语义和行为边界，不以其整个类替换3.1实现。

| 最终决定 | PR来源 | 纳入目的 | GridGain核查 |
|---|---|---|---|
| 纳入核心恢复链 | #7286、#7242、#7507、#7813 | 等待Raft组恢复并真正接通MetaStorage getAll/invoke，预防本次默认预算超时击穿Watch的机制 | 实际调用链与字节码已确认存在关键等效实现 |
| 纳入完整CMG客户端迁移 | #7509 | CMG实际接入TimeAware客户端、超时接口与调用点适配，以及供#7813复用的工厂和RaftManager/Loza入口 | GridGain的CMG与MetaStorage均实际接入新客户端，CMG补充字节码核验已完成 |
| 纳入故障停止 | #7235、#7563 | 不可恢复错误触发异步stop，处置失效节点继续在线 | 默认stop与独立线程stop由原始JAR探针确认 |
| 纳入停止策略配套 | #7517、#7554 | 防止事务结束竞争和vacuum超时触发不必要的节点停止 | 对应核心处理已确认存在 |
| 纳入重试正确性配套 | #6903、#6954 | 快照恢复后的幂等结果类型、重试时命令对象与safeTime隔离 | 对应实现已由原始JAR字节码确认 |

这些配套项是本次交付的明确建议范围，不表示每项都是#7813编译上的硬依赖，也不表示现场已经命中每个缺陷。#6903/#6954针对恢复重试的正确性；#7517/#7554针对启用全局默认stop后的行为边界。

**本次必选清单不包含#7287与#7787**：前者为旧客户端计时健壮性增强，现场未证明时钟跳变；后者仅减少重复日志。可以独立作为可选小项评估，不影响主修复是否完成的判定。#6243、#3725、#3139已包含，不重复计入。Primary分布/手动均衡、DataNodes手动重算REST和Parquet不纳入此次Watch修复。

实施顺序：先适配#6903/#6954，随后完成新Raft客户端→#7509完整CMG客户端切换→#7813 MetaStorage接入；再完成#7235和#7517/#7554，最后启用#7563并核对显式配置。最终发布须通过CMG、MetaStorage恢复与故障停止的验收。

### 为什么调整#7509范围

此前建议部分提取，是为了优先修复直接观察到的MetaStorage超时链、减少启动/入群等控制面回归范围；这属于工程范围取舍，不是#7509不能在3.1回移的技术结论。将它直接列为最终限制缺少充分依据。

重新核对完整diff后，#7509主要修改CmgRaftService、ClusterManagementGroupManager的客户端类型、超时参数、工厂和接线，附带相关测试；没有在该提交中修改CMG命令字段、状态机持久化格式或Raft选举算法。当前这两个CMG核心类与官方3.1.0无diff，未发现要求保留旧客户端的本地改动。以上支持完整语义回移的可行性，不等于已经编译和混部验证通过。

GridGain 9.1.28也不是只给MetaStorage接新客户端：其ClusterManagementGroupManager实际创建PhysicalTopologyAware工厂，通过TimeAware入口启动CMG，CmgRaftService使用TimeAware.run。补充对原始ignite-cluster-management JAR计算SHA-256并核验两个类的javap，证据见本目录`gridgain-cmg-identity.json`及`gridgain-*.javap.txt`。

在客户端依赖已纳入的前提下，完整#7509可使CMG与MetaStorage对暂时失去多数派采用一致的客户端恢复框架，减少自行拆分补丁的长期维护。代价是明确增加CMG逐节点启动、初始化/入群、逻辑拓扑与learner变化、多数派丢失/恢复、有限超时查询及等待期间停机的回归。这些必须随实现完成。

这里“完整”限定为#7509的完整功能语义及其3.1适配，不包含GridGain整套CMG模块或其他后续功能；内部NO_TIMEOUT与外部调用的有限超时边界仍需保留。它也不会恢复已经失败的Watch future。

## 最终结果判定

### 最终判定：有条件能够解决当前故障机制，尚未验证解决；仅回移PR还不能保证完整自动恢复

**针对当前最有证据支持的故障机制——MetaStorage短暂失去Leader/多数派，恢复后部分请求仍因默认重试预算耗尽而异常，继而击穿DataNodes和Watch——A组恢复链预期能够阻断这一触发路径；B组用于在仍发生不可恢复错误时停止异常节点。因此，这组方案可以作为本次Watch问题的修复方案推进，但当前不能标记“已解决”。**

这个判定有两个前提：现场首次DataNodes失败确实属于该类可重试的MetaStorage超时；3.1回移后的实际调用链、重试与停止行为通过故障复现验证。现场完整cause尚未逐trace闭环，回移也尚未实施，故现阶段依据是源码机制覆盖，而不是运行结果。

| 用户关心的最终结果 | 最终评估 |
|---|---|
| 暂时MetaStorage故障恢复后，Watch继续处理后续revision，LeaseTracker恢复更新 | **预期可以**，关键是#7813及新Raft客户端依赖真正接入；尚需复现验证 |
| Watch真正发生不可恢复错误后，节点不再长期在线接收请求 | **预期可以通过停止节点实现**，依靠#7235+#7563及停止策略配套，前提是策略实际生效、停止完成 |
| 目前已经失败的Watch链，在原节点进程内自行恢复 | **不能**，这些PR不修复已经异常完成的通知future；需停止并重新启动节点来重建状态 |
| 故障节点自动重启，并追赶revision、恢复Lease/Primary查询 | **仅靠这些PR不能保证**；还需要验证或补齐普通Watch故障的JVM/容器拉起与恢复闭环，已有stale专用机制不覆盖此路径 |
| 永久丢失多数派、任意Watch监听器异常都不再影响请求 | **不能保证**；内部NO_TIMEOUT可能持续等待，外部请求仍需有限超时，永久多数派丢失需恢复集群可用性 |

**Hotfix 6的最终交付标准**应是：在复现当前超时链后，集群恢复即能继续推进Watch/revision/Lease；若出现不可恢复的Watch异常，节点能够在规定时限内退出服务，并经明确的重启机制恢复和追赶状态；持续读写的已确认提交结果正确。只有这些场景实际通过，才能写“当前Watch故障已解决”。仅测试PR编译通过、节点重新加入拓扑或请求不再报原异常，都不足以得出这个结论。

### 回到Watch本身：本次具体修复什么

**有针对性覆盖，但尚不能判定现场问题已被完整解决。** 对已观察到的“暂时MetaStorage不可用/选主波动→请求默认预算耗尽→DataNodes异常→Watch永久异常”机制，#7813及新客户端链提供了直接覆盖：把内部操作从一次预算耗尽后failed，改为跨暂时不可用继续等待并在恢复后完成。#7509把同一框架接入CMG，增强控制面的整体恢复一致性；它单独不是DataNodes Watch根因补丁。

| 内部操作状态 | 后续行为 |
|---|---|
| 旧路径：异常完成（failed）并传播到串联Watch future | 即使MetaStorage恢复，后续revision通知也不会重新执行 |
| 新恢复路径：可重试故障期间保持等待（pending） | 暂时故障恢复后完成原操作，后续revision通知可以继续 |
| 新路径但多数派永久不恢复 | 内部操作仍可能等待，Watch也可能暂不推进；业务调用必须有界超时，不能把它算作已解决永久不可用 |
| 不可恢复异常仍导致Watch failed | #7235+#7563处置异常节点；原进程不会原地修复Watch，自动拉起与追赶仍需另外闭环 |

因此本次重点是**阻止暂时不可用被放大为永久本地状态失效，并在不可恢复时终止异常节点**。它不消除节点重启、网络抖动、所有多数派丢失，也不修复已有磁盘损坏。现场首个cause尚未唯一关联，当前也未运行3.1回移候选的事故复现，故不能把代码覆盖判断升级为已验证解决。

### 与之前cluster启动/恢复问题的区别

“之前启动问题”在历史中有多类，这里按最近的保留work恢复、Catalog/LWM与stale入群修复分别对照。阶段只是现象；需要区分具体出错状态和修复落点。stale冲突也可能由运行中网络隔离触发，不能一概归为纯启动故障。

| 问题 | 首个缺口/停滞状态 | 既有修复与验收目标 | 与本次Watch的关系 |
|---|---|---|---|
| 快照中断后保留work重启、term/vote丢失 | reset/destroy清理范围错误，持久选举状态被删 | reset只清目标日志/快照，保留raft_meta；验证KILL后term/vote及数据恢复 | 修复持久恢复状态；不改变MetaStorage请求超时或Watch失败future |
| CatalogNotFoundException导致节点启动退出 | Vault恢复的旧LWM早于仍保留的Catalog历史 | #7330启动校正并持久化LWM，调整消费者启动顺序 | 修复启动时元数据时间边界；不负责运行期Watch恢复 |
| LWM等待已完成A却等待被锁住的B | 完成条件绑定错误的候选future | 按请求阈值及已完成进度结束等待，隔离取消；验证实际快照恢复和再次重启 | 同样涉及future，但对象是LWM更新；不是MetaStorage Watch通知链 |
| stale launchId拒绝、无法重连入群/形成多数派 | 旧节点身份被拒绝且仍复用，拓扑版本落后 | #7248/#7977核心语义加本地兼容握手、退出码75、持久预算和监督拉起 | 解决身份冲突及特定恢复；普通Watch异常不会自动进入stale专用路径 |
| 本次：节点仍在线但revision/Lease不再推进 | MetaStorage操作异常传播后，Watch future链永久failed，noop保留节点 | 新客户端+#7813预防暂时超时击穿；异步stop处置不可恢复错误 | 恢复运行期元数据应用能力；已坏的Watch仍需重启 |

**旧恢复验收通过不等于本次Watch链已修复，本次Watch修复也不能替代旧修复。** 历史报告显示：meta/LWM候选完成过保留work重启、真实快照恢复、三副本数据校验；stale候选完成过有预算的隔离/重连验收。这些有明确验证边界，不覆盖本次“DataNodes失败后Watch永久异常”的事故时序。

### 之前测试中暴露但尚未修复的MetaStorage问题

不能把所有历史问题都写成已经解决。2026-10-21的`artifacts/hotfix4/metastorage-attribution-20261021/REPORT.md`明确记录：DefaultMessagingService发送/连接阶段的返回future可能超出响应超时；复制器持锁同步connect，ReadIndex等锁再阻塞写入；健康多数派仍在时也可触发MetaStorage读写超时。修复前后基线均复现，当时**只完成归因，没有实施修复**。这与本次属于相关的MetaStorage暂时不可用/超时问题族，但没有证据证明现场底层原因完全相同。

本次11个PR来源没有修改DefaultMessagingService、AbstractClientService、Replicator或NodeImpl中的上述根因路径。新RaftCommandExecutor给组合invoke future增加orTimeout，可改善其调用方对单次RPC超时的观察边界，并以重试等待跨过暂时阻塞；**这不等于消除服务端复制器持锁连接和ReadIndex锁阻塞。** GridGain中对应底层机制也仍可见，不能用“GridGain已有新客户端”推断底层网络/锁问题已消失。

故最终验收除本次事故时序外，还需重跑“健康多数派＋KILL一个follower”的旧最小A/B场景，检查单次超时、恢复后持续Watch/revision/Lease推进、停机完成与在途请求资源释放。若要求消除底层网络/锁阻塞本身，应另列修复范围及验证，不能凭这11个PR宣称根治。

历史证据：`artifacts/hotfix4/meta-lwm-fixes-20261021/RUNTIME_VALIDATION_REPORT.md`、`CLUSTER_VALIDATION_REPORT.md`；`artifacts/hotfix4/rolling-latest-meta-lwm-20261021/REPORT.md`；`artifacts/hotfix4/catalog-lwm-7330-20261015/REPORT.md`；`artifacts/hotfix4/stale-safe-exit-fixes-20261019/REPORT.md`；`artifacts/hotfix4/metastorage-attribution-20261021/REPORT.md`。历史验收中的切换期请求错误与未完成项目仍保留，不据此改判全程零失败。

**不能只引入 #7563 + #7235。建议同时规划“MetaStorage跨暂时不可用恢复”与“不可恢复错误停止节点”两组官方语义；启用默认stop前还需处理官方已确认的不应触发失败处理的事务路径。**

本轮新增的关键发现是 **IGNITE-27162 / #7813**。它将MetaStorage切换到TimeAwareRaftGroupService，并在MetaStorageManager内部的getAll、invoke等调用使用NO_TIMEOUT，从而不再仅因旧客户端默认重试预算耗尽而让这些内部操作异常结束。该改动与本次“Leader恢复了，部分在途请求稍后超时，DataNodes/Watch被击穿”的机制直接相关。

这更新了上一轮候选清单的覆盖范围：上一轮针对IGNITE-27801“没有确认修复”的判断仍成立，但不能据此推导为官方不存在其他可覆盖该机制的后续方案。本轮沿Raft客户端演进和MetaStorage实际调用点补查到了#7813。

## 建议引入的组合

| 分组 | Jira / PR | 作用与决定 |
|---|---|---|
| A：核心恢复 | [IGNITE-27434 / #7286](https://github.com/apache/ignite-3/pull/7286) | 引入等待组可用的新Raft客户端、TimeAware接口；核心前置 |
| A：核心恢复 | [IGNITE-27156 / #7242](https://github.com/apache/ignite-3/pull/7242) | 实现Leader可用状态与RaftCommandExecutor，改变超时和重试处理；核心前置 |
| A：核心恢复 | [IGNITE-27714 / #7507](https://github.com/apache/ignite-3/pull/7507) | 完成其他Raft操作的超时、重试和订阅处理；采用后续完成版语义，不能只停留在#7286 |
| A：CMG客户端迁移 | [IGNITE-27157 / #7509](https://github.com/apache/ignite-3/pull/7509) | 完整适配CMG的TimeAware客户端、超时接口、调用点及测试；同时提供#7813复用的TimeAware工厂和RaftManager/Loza入口 |
| A：实际接入 | [IGNITE-27162 / #7813](https://github.com/apache/ignite-3/pull/7813) | **最直接覆盖当前触发机制**：MetaStorageService/Manager接新客户端，内部getAll/invoke等持续等待恢复；必须真正接通生产调用链，单加客户端类不算完成 |
| B：故障停止 | [IGNITE-27324 / #7235](https://github.com/apache/ignite-3/pull/7235) | 在独立且具备Ignite线程操作权限的线程停止节点，避免在故障/Watch回调线程中同步停机死锁；先适配它，再启用默认stop |
| B：故障停止 | [IGNITE-21456 / #7563](https://github.com/apache/ignite-3/pull/7563) | 默认noop改stop，处置仍然发生的不可恢复错误。已有显式noop配置必须另行核对，改默认值不会覆盖它 |
| B：停止策略配套 | [IGNITE-27702 / #7517](https://github.com/apache/ignite-3/pull/7517) | 事务结束时结果竞争/已有结果不应从Raft应用路径升级为节点失败。当前分支保留UnexpectedTransactionStateException旧路径；建议在默认stop前配套适配 |
| B：停止策略配套 | [IGNITE-27720 / #7554](https://github.com/apache/ignite-3/pull/7554) | vacuum因复制超时等失败改为限流告警，避免停止节点。当前PersistentTxStateVacuumizer仍直接调用failureProcessor；建议在默认stop前配套适配 |
| C：重试正确性 | [IGNITE-26942 / #6903](https://github.com/apache/ignite-3/pull/6903) | 修复MetaStorage幂等缓存从快照恢复后Boolean/StatementResult类型错误。不是已证明的现场cause；考虑重启、快照恢复与重试场景，建议同一候选配套引入 |
| C：重试正确性 | [IGNITE-27035 / #6954](https://github.com/apache/ignite-3/pull/6954) | beforeApply克隆写命令后再修改safeTime，避免重试复用已处理命令的并发修改。本地相关实现仍是3.1.0；建议与恢复链一起验证和引入 |

推荐范围是A、B、C三组；A组涉及5个PR来源，B组4个，C组2个。**这些是11个PR的语义来源，不代表能无冲突顺序cherry-pick的11个独立补丁。** #7509按完整CMG客户端迁移语义适配，完整实现仍需编译、API兼容和行为依赖核对。

## 核心依赖关系与适配范围

```text
#7286 → #7242 → #7507 ─────────────────────┐
                                         ├→ #7813：MetaStorage真实接入
#7509：CMG客户端迁移、工厂及RaftManager/Loza入口 ┘

#7517 + #7554：避免已知非致命事务路径触发停机
#7235：异步且有操作权限的节点停止
                 ↓
#7563：启用默认stop

#6903 + #6954：重试、重启/快照恢复时的配套正确性
```

这是源码语义依赖关系，并非列出上游全部历史提交。Raft客户端涉及数千行重构与多个API，不能把Hotfix 6描述为改一个配置项；建议分两批开发提交，但在声明“覆盖本次触发与长期后果”前必须完成两条主链的验证。

- 先完成C组的重试正确性修复，再适配A组新客户端、#7509完整CMG客户端切换与#7813 MetaStorage接入，验证CMG与MetaStorage暂时丢失多数派后的恢复，以及DataNodes/Watch继续推进。
- B组先完成#7235与#7517/#7554适配，最后启用#7563，验证真正故障安全停止、事务竞争/vacuum超时不导致误停止。单独完成B组只能称故障处置改善。
- 保留当前分支已有的恢复位点、异步等待、低水位保护和stale代际/预算机制；禁止用上游整文件覆盖本地文件。
- 范围以MetaStorage客户端和失败生命周期为主；不主动修改Raft选举算法、分区数据路由、Primary均衡、磁盘格式或快照协议。若适配实际要求改变这些范围，必须重新评估，不能隐含扩张。

## 能解决什么，不能承诺什么

### A组的预期覆盖

对于“暂时没有Leader/多数派，随后恢复”的可重试故障，内部元数据操作不因10秒或30秒默认预算耗尽而永久异常，应在组恢复后继续推进，保持Watch通知的顺序约束。这是基于实际diff的适用性判断，尚非当前3.1候选上的运行验证。

原始扰动的物理原因（node1为何重启、node0为何短暂无有效Raft响应）没有因此消除。#7813也不会让已经异常完成的Watch future在原进程内自动修复，不覆盖所有DataNodes业务/数据异常。

### B组的覆盖和自动恢复边界

发生真正不可恢复错误时，默认stop可避免异常节点继续提供服务；#7235避免在回调线程同步停止造成的已知死锁。但StopNodeFailureHandler本身没有强制停止时限，不能承诺任意关闭挂起都被解决。

**停止Ignite节点不等于保证JVM退出或容器重启。** 当前FailureManager普通stop回调是`node::shutdown`；现有入口监督器只在专用退出码75时重拉，且该码由StaleNodeRecovery专用路径产生。普通Watch错误不会进入该恢复路径。

如果本次验收要求自动恢复，必须额外验证单节点部署中“停节点→JVM/容器状态→编排平台拉起→复用原work恢复→revision和Lease继续推进”的闭环。若需要新增退出/健康检查/监督逻辑，应明确标注为本地新增工程能力，保留有界重启、退避与实例代际保护，不能称为#7563/#7235自带能力，也不能直接复用stale专用码75。

### NO_TIMEOUT的边界

NO_TIMEOUT是内部一致性状态机操作的等待策略。永久丢失多数派时操作仍可能持续等待；它不是无限等待也能成功的承诺，更不能直接作为业务HTTP/RPC超时策略。外层请求仍要有限超时/快速失败；调用方超时不能把底层共享元数据操作永久取消或缓存为失败。停机时必须结束等待并释放资源。

## 不作为主修复的候选

- [#7287 / IGNITE-27436](https://github.com/apache/ignite-3/pull/7287)：建议作为保留旧Raft客户端路径的计时健壮性增强单独评估。新RaftCommandExecutor本身使用单调时间；现场没有时钟跳变证据，不能拿它替代A组。
- [#7787 / IGNITE-28235](https://github.com/apache/ignite-3/pull/7787)：可选诊断改善，只减少Watch错误重复日志，不提供恢复能力。
- [#6243 / IGNITE-25848](https://github.com/apache/ignite-3/pull/6243)：已在当前基线，仅测试组织/超时和停止顺序改进，不是生产请求恢复补丁。
- [#3725 / IGNITE-21881](https://github.com/apache/ignite-3/pull/3725)：已在基线，不能算Hotfix 6新增修复。
- #3139已继承自3.1.0，针对其他监听路径，不替代A组。
- IGNITE-27801仍是Cannot Reproduce，无已确认修复PR；它只是相似机制证据。
- 手动DataNodes重算REST #6902不修复失败的Watch链，不应重新混入本次Hotfix 6。

## 必需验收（计划，尚未执行）

| 场景 | 验收要求 |
|---|---|
| 三节点Leader退位并重选；在途RPC等待旧/重启peer | 人工制造失败先返回、恢复后旧RPC才超时的时序；恢复后DataNodes/Watch继续处理后续revision，而非仅查看集群重新选出Leader |
| 故障持续超过原10秒和30秒预算后恢复 | 内部更新能够恢复；外部请求按自身期限返回；不因一次超时形成永久失败future |
| 长期丢失多数派 | 业务请求有界结束；内部等待不形成无界资源积累；节点停机不挂起；不因等待而同时强制重启全部健康节点 |
| 写入已提交但响应丢失，之后重试 | 命令ID/幂等语义保持；DataNodes历史和计时器无错误重复、无revision跳过，safeTime无重排/重复导致的错误 |
| MetaStorage快照恢复后重复MultiInvoke | 验证幂等缓存结果类型和操作结果一致，覆盖#6903；验证命令克隆隔离，覆盖#6954 |
| 注入不可恢复Watch异常 | FailureManager只执行一次有效终止，停止线程不自锁；节点退出服务；若要求自动恢复，必须验证真实进程/容器拉起和Lease/revision追赶 |
| 事务竞争、重复finish及vacuum复制超时 | 返回原事务结果或业务冲突、后续可重试；不把这些官方已区分的错误升级为节点终止 |
| PD/Primary切换且持续读写 | 对已确认提交的数据校验结果，不只检查请求成功率；故障后恢复、客户端超时与最终提交状态分开记录 |
| 新旧hotfix滚动混部、重启后再次故障 | 核对消息编码、异常码、命令序列化和存储兼容；第二轮仍可恢复，无旧代际停止新实例 |
| CMG逐节点启动、初始化与入群、多数派丢失后恢复 | 验证初始化/加入/拓扑及learner更新继续推进；带有限timeout的查询按期结束，内部持续等待不导致停机挂起 |
| 显式noop配置、默认配置、停止中再报错 | 确认策略实际生效与忽略类型边界；保留现有stale专用恢复逻辑且不串用预算/退出码 |

## 证据与状态

GitHub连接本轮核验上述主要PR已经合并；提交patch和元数据保存在本目录。#7813合并时间为2026-04-08。公开Jira页面的搜索缓存仍显示3月的Patch Available状态，与GitHub实时合并元数据不一致；采用GitHub合并状态，保留Jira对动机和依赖的描述。

[IGNITE-27162](https://issues.apache.org/jira/browse/IGNITE-27162)明确说明MetaStorage原先依赖默认Raft超时，要求改用新客户端，并关联IGNITE-27156与IGNITE-26085。[IGNITE-27714](https://issues.apache.org/jira/browse/IGNITE-27714)说明#7286后的其他操作需要完成同样的超时处理。

关键静态核对：#7813在MetaStorageManager中实际修改getAll/invoke为NO_TIMEOUT，并在IgniteImpl注入PhysicalTopologyAwareRaftGroupServiceFactory；工厂及RaftManager/Loza入口来源#7509。当前MetaStorageService仍使用旧run(command)；当前vacuum、finish路径仍含上述旧处理。#6903/#6954相关文件与原3.1.0无diff。NetworkMessage已有clone接口，但完整行为仍须适配和验证。

`local-commit-inventory.json`记录祖先关系；非祖先不能单独证明未回移，以上结论结合实际实现。此次没有修改运行源码，没有运行测试，不能把建议标为已验证解决。
