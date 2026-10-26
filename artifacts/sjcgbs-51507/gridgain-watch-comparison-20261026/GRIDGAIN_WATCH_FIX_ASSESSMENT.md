# GridGain 9.1.28 对当前 Watch 故障的修复覆盖核查

核查日期：2026-10-26。目标是本地GridGain **9.1.28** 发布包，不代表其他版本或生产实际部署。对照对象为官方PR评估和当前Ignite 3.1 hotfix基线`65c9420c2ca771d70ff96301b4caa41c8f643c96`。

## 最终结论

**GridGain 9.1.28 已落地与本次故障最相关的两层防护：MetaStorage内部操作持续等待组恢复，避免默认重试预算耗尽击穿DataNodes/Watch；不可恢复故障默认异步停止节点，避免noop让异常节点长期在线。** 这两层关键实现及重试正确性、停止策略配套修复均可在包内实际代码中确认。

**它没有实现已经失败的Watch链在原进程内自动恢复。** 原始JAR的组件探针确认，一次通知异常后，后续入队action不会执行，故障处理器只通知一次；字节码保留了“后续revision不会通知，直到restart”的明确错误文本。

因此，对“GridGain是否修复我们的问题”的准确回答是：

- **对于本次推断的“暂时MetaStorage不可用→默认请求超时→Watch永久失效”机制，已包含对应预防修复和故障停止兜底。** 这是基于生产调用链和包内字节码的静态覆盖结论，尚未在真实三节点复现事故时序验证。
- 对“任何异常后Watch都能原地自愈”“失败节点一定自动重启”“永久多数派丢失时业务仍能成功”，**不能据此判定已完成**。
- 对已经Watch失效的当前节点，升级/重启后会使用新实现；不能期待替换JAR前原进程中的失败future自行恢复。

## 证据来源与可信度

输入：`/Users/chenmingjie/ths/gridgain-9.1.28/gridgain9-db-9.1.28/lib/`，实际发布包JAR。

源码阅读视图：`/Users/chenmingjie/ths/ignite-3/artifacts/gridgain-9.1.28-decompiled/modules/`。该目录是Vineflower反编译结果，不是GridGain原始源码仓库。

本轮对8个相关JAR计算SHA-256，全部与各模块ORIGIN.json一致；对18个原始class执行`javap -c -p`，全部成功；扫描发布包全部JAR确认这18个类的来源均唯一，避免classpath同名类影响结论。身份和字节码分别保存在`artifact-identity.json`、`class-origin-check.json`和`*.javap.txt`。

其中`ignite-metastorage-9.1.28.jar` SHA-256为：

`3796832ac0d910238cfca770aa25334d6511152b03d52b8ef06678a0f38656b6`

REA制品盘点与身份Evidence已导出到`rea-evidence.json`。REA只用于制品盘点；方法语义依据是反编译结果、原始JAR字节码及下述组件探针。

## 与建议PR的逐层对照

下表“等效实现存在”表示观察到对应行为，不声称取得GridGain私有Git历史或证明原样cherry-pick了某个Apache提交。

| 检查项 / 官方PR参照 | GridGain 9.1.28发现 | 判定 |
|---|---|---|
| 新Raft客户端：#7286、#7242、#7507 | TimeAwareRaftGroupService、RaftCommandExecutor、LeaderAvailabilityState等实际类存在；PhysicalTopologyAwareRaftGroupService.run委托新executor；executor有等待Leader并重试、单调计时、停止时结束Leader等待的逻辑 | 关键等效机制已存在 |
| MetaStorage真实接入：#7813及#7509必要接线 | IgniteImpl实际创建PhysicalTopologyAwareRaftGroupServiceFactory并传入MetaStorageManagerImpl；Manager内部getAll/invoke传Long.MAX_VALUE；Service将显式timeout传给TimeAware.run | **本次最关键的触发防护已接通**，不是仅JAR里存在未使用的新客户端类 |
| 默认noop改stop：#7563 | FailureHandlerConfigurationSchema构造器给type赋值`stop`；组件探针确认 | 已覆盖，显式配置仍可覆盖默认值 |
| 独立线程停止：#7235 | StopNodeFailureHandler用IgniteThreadFactory创建node-stopper线程后start；探针确认callback线程不是调用线程 | 已覆盖对应同步停机问题；未证明所有停止都不会挂起 |
| 幂等缓存类型修复：#6903 | cachedResult为Boolean且命令为MultiInvoke时，构造StatementResult返回 | 等效修复已存在 |
| 重试共享命令safeTime竞争：#6954 | beforeApply调用command.clone后再写safeTime | 等效修复已存在 |
| vacuum失败不终止节点：#7554 | 失败路径使用throttledLogger.warn，不再调用FailureProcessor处理该错误 | 等效修复已存在 |
| 事务结束竞争处理：#7517 | CAS失败时返回已存TransactionResult，业务冲突与Raft应用异常分开；不再采用旧UnexpectedTransactionStateException路径 | 等效核心处理已存在 |
| Watch失败重复报告：#7787 | firedFailureOnChain.compareAndSet(false,true)控制首次处理；探针确认一次通知 | 已覆盖；作用不是恢复Watch |
| Watch失效后的原地恢复 | notificationFuture仍由thenComposeAsync串联；真正失败后仍要求restart | **未提供**，组件探针直接确认后续action不执行 |
| 普通Watch故障自动拉起 | FailureManager由IgniteImpl注入node::shutdown；检查路径没有改成restartAsync。发布ZIP启动脚本最终单次eval Java命令，没有监督重拉循环 | **本次检查的路径不提供自动拉起保证**；Docker/Kubernetes/外部运维平台策略未包含在本次运行验证内 |

## 关键代码位置（反编译阅读视图）

均相对于上述`modules/`目录，行号仅用于阅读，不是原厂源码行号：

- `runner/src/main/java/org/apache/ignite/internal/app/IgniteImpl.java:770–796`：新MS工厂实际接入；`:614`：普通故障stopper为node::shutdown。
- `metastorage/src/main/java/org/apache/ignite/internal/metastorage/impl/MetaStorageManagerImpl.java:795–800`：getAll使用Long.MAX_VALUE；`:896–903`：DataNodes使用的invoke(Iif)同样使用Long.MAX_VALUE。
- `metastorage/src/main/java/org/apache/ignite/internal/metastorage/impl/MetaStorageServiceImpl.java:88–90,154–162`：timeout实际传入新Raft客户端。
- `raft/src/main/java/org/apache/ignite/internal/raft/client/PhysicalTopologyAwareRaftGroupService.java:199–202`：run委托commandExecutor。
- `raft/src/main/java/org/apache/ignite/internal/raft/client/RaftCommandExecutor.java:100–118,368–407,647–664`：总期限、单次RPC、组不可用时等待Leader再重试；内部持续等待不等于单次网络RPC永远不结束。
- `distribution-zones/src/main/java/org/apache/ignite/internal/distributionzones/DataNodesManager.java:761–764,841–895`：上下文远程读和invoke仍走MetaStorageManager；未恢复的异常仍会上报失败。
- `failure-handler/src/main/java/org/apache/ignite/internal/failure/handlers/configuration/FailureHandlerConfigurationSchema.java:11`：默认stop。
- `failure-handler/src/main/java/org/apache/ignite/internal/failure/handlers/StopNodeFailureHandler.java:24–28`：独立线程停止。
- `metastorage/src/main/java/org/apache/ignite/internal/metastorage/server/WatchProcessor.java:111–123,259–273`：串联future、首次失败处理与要求重启。

字节码再核对：`MetaStorageManagerImpl.javap.txt:2432,2522`对应invoke和getAll的lambda，明确加载`9223372036854775807L`并调用带timeout的Service方法；`StopNodeFailureHandler.javap.txt:21`起明确调用newThread/start；`WatchProcessor.javap.txt:653`附近保留until a restart文本并调用FailureProcessor。

## 原始JAR组件探针：已执行

探针文件：`WatchRecoveryProbe.java`。使用发布包完整lib classpath编译和运行，不重新编译或替换GridGain类，不启动Ignite节点，不连接集群，不操作其他容器。

实际结果：

```json
{
  "default_handler": "stop",
  "stop_callback_on_separate_thread": true,
  "successful_control_actions": 1,
  "subsequent_action_after_failure_executed": false,
  "failure_notifications": 1,
  "scope": "isolated-components-not-cluster-recovery",
  "passed": true
}
```

退出码0，原始stdout/stderr和汇总见`probe-result.json`。先执行一个成功action作为正对照，再注入异常，之后入队的action继承异常且没有执行。Watch部分用记录型FailureProcessor代替实际停机，使探针能够隔离观察通知链；独立的StopNodeFailureHandler探针用记录型NodeStopper验证异步线程。因此该探针不证明真实节点停机、JVM退出、容器重启或集群恢复成功。

没有执行GridGain三节点网络故障、持久数据恢复、PD切换、持续读写回归，不能将这些记为通过。

## 对Hotfix 6的影响

GridGain代码支持上一轮选择#7813及新Raft客户端依赖作为核心修复方向：它的生产路径确实采用这一组合；默认stop与事务/重试配套修复也已存在。可以用Apache官方PR作为回移来源，以GridGain的实际接线和异常边界作为交叉核查依据。

Hotfix 6的目标应明确为：暂时不可用时保留内部操作并在恢复后继续处理；真正不可恢复时停止异常节点；若承诺自动恢复，则另行交付普通Watch故障的有界退出/重启与追赶验证。不要把“GridGain包含这些修复”写成“GridGain保证任何Watch异常都不需要重启”。

现有失败链仍要通过重启重建；永久多数派丢失时内部操作可能持续等待，外层业务请求仍需有界超时。现场首次DataNodes完整cause尚未唯一关联的边界也不因本次代码对照而消失。
