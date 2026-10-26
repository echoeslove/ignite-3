# SJCGBS-51507 / Hotfix 6 实施与验证结果

日期：2026-10-26。基线 `65c9420c2ca771d70ff96301b4caa41c8f643c96`，分支仍为 `hotfix-4`，改动留在工作区，未提交、未发布、未部署生产。

## 最终结果与适用边界

已完成评估中 11 项官方 PR 的 3.1 兼容回移，**包含完整 CMG 客户端迁移**，以及 MetaStorage 对新客户端的实际接线。不是只增加工厂或只修改默认停止策略。

**对于短暂 MetaStorage 请求失败／选主恢复耗时超过旧客户端默认重试预算、进而击穿 DataNodes Watch 的路径，本次验证得到预期结果：内部请求保持等待，恢复后 DataNodes 与后续 Watch revision 继续应用，Lease 与业务读写恢复推进。**

对于真正不可恢复的 Watch 监听器异常，**Watch 链仍不提供原地自动恢复**。默认策略改为在独立线程停止故障节点，避免失效节点继续在线服务；本轮已验证同工作目录重启后追平。测试夹具负责重启，没有增加普通 Watch 故障的 JVM／容器自动拉起策略。生产首次触发 DataNodes 错误的完整异常链仍未闭环，本轮合成故障不能证明覆盖所有实际首因。

## 实施范围与官方来源

代码来自本地 Apache 官方 Git 提交；GridGain 9.1.28 用于前序语义与接线对照，本次没有以 GridGain 整个反编译类替换 Ignite 3.1 类。

| 官方 PR | 官方提交 | 本次实现 |
|---|---|---|
| [#6903](https://github.com/apache/ignite-3/pull/6903) | `a021050f8d` | 幂等命令缓存从快照恢复后，正确还原 MultiInvoke 的 Boolean／StatementResult 类型 |
| [#6954](https://github.com/apache/ignite-3/pull/6954) | `3eceb6e0f6` | BeforeApply 先克隆命令，再修改 safeTime；RPC 应用与序列化使用克隆后的对象 |
| [#7286](https://github.com/apache/ignite-3/pull/7286) | `04fc38f6f3` | TimeAware Raft 客户端基础 |
| [#7242](https://github.com/apache/ignite-3/pull/7242) | `3c4439be78` | Leader 可用性等待、重试阶段与组不可用处理 |
| [#7507](https://github.com/apache/ignite-3/pull/7507) | `0cc773ce87` | 配套完成的新客户端实现 |
| [#7509](https://github.com/apache/ignite-3/pull/7509) | `5fc56a61e8` | CMG 完整迁移、超时调用接口、共享选主事件监听器、工厂及 RaftManager／Loza 接线 |
| [#7813](https://github.com/apache/ignite-3/pull/7813) | `4facea77b6` | MetaStorage get／getAll／invoke／扫描／恢复等实际使用新客户端；内部调用显式 NO_TIMEOUT |
| [#7235](https://github.com/apache/ignite-3/pull/7235) | `8981fa201f` | 在独立 Ignite 线程执行停节点，避免故障通知线程同步停机死锁 |
| [#7563](https://github.com/apache/ignite-3/pull/7563) | `5ed38e852e` | 缺省 failureHandler 从 noop 改为 stop |
| [#7517](https://github.com/apache/ignite-3/pull/7517) | `89c8d45a2e` | 事务完成竞争返回既有结果，向调用方报告结局不匹配，避免作为状态机异常停止节点 |
| [#7554](https://github.com/apache/ignite-3/pull/7554) | `8a09f05582` | 持久事务 vacuum 失败限频告警、保留待重试事务，不调用 FailureProcessor 停节点 |

新客户端文件以 `4facea77b6` 时的最终实现为整合来源，配合上述提交在现有类中适配。没有纳入 Primary 观测／重平衡 REST、Parquet 优化、可选日志去重 #7787 或旧客户端时钟改造 #7287。

### 3.1 兼容处理

- 保留旧 RaftGroupServiceImpl 与分区客户端；CMG、MetaStorage 使用新客户端，分区不整体迁移。
- 3.1 成员变更 RPC 没有上游后续版本的 sequenceToken。本次新接口和调用点沿用 3.1 参数形态，不修改消息定义、序列化布局或磁盘格式。
- 新客户端 ESTALE 映射到 3.1 已有 RaftException，仍按终止性错误处理；没有带入后续成员变更特性。
- 增加官方 GROUP_UNAVAILABLE_ERR（Replicator 错误码 10）和饱和的 monotonic deadline 运算。
- 保留现有 V1／V2 FinishTx 命令分派、applied-index 防护、MetaStorage divergence／snapshot／LWM／stale 恢复代码。
- Loza 新入口只启动一次服务端并创建一次新客户端；已去掉适配初版中多余创建的旧客户端，并使用配置后的命令 marshaller。

## 验证结果

发行包 `packaging/db/build/install/ignite3-db` 构建成功。编译使用 JDK 21；公式 FFM 运行使用 JDK 25。最终测试按 suite／testcase 统计并保留 XML：**677 通过、0 失败／错误、2 个既有禁用用例跳过**；去重后总计 679 个注册用例。未将多次重跑或旧日期 XML 加入通过数量。

| 模块与测试类型 | 通过 | 跳过 | 失败／错误 |
|---|---:|---:|---:|
| cluster-management/integrationTest | 9 | 0 | 0 |
| cluster-management/test | 99 | 0 | 0 |
| failure-handler/integrationTest | 2 | 0 | 0 |
| failure-handler/test | 22 | 0 | 0 |
| metastorage/integrationTest | 46 | 2 | 0 |
| metastorage/test | 371 | 0 | 0 |
| partition-replicator/integrationTest | 6 | 0 | 0 |
| partition-replicator/test | 2 | 0 | 0 |
| placement-driver/integrationTest | 1 | 0 | 0 |
| raft/integrationTest | 3 | 0 | 0 |
| raft/test | 51 | 0 | 0 |
| transactions/test | 65 | 0 | 0 |

两个跳过项是原有 `ItMetaStorageServiceTest` 的 IGNITE-14693 异常处理测试，未新增或扩大禁用范围。有限超时／等待／重试语义由新客户端定向测试覆盖；真实网络阻断测试覆盖超过旧重试预算后的恢复。

新增或复用的关键验证包括：快照恢复 Boolean true／false、重试对象隔离、Raft 选主通知与退出、CMG 网络服务、真实 RocksDB 快照／删目录恢复、选主后扫描游标连续性、PD 活跃 actor 切换后 Lease 续约、事务结束复制与 vacuum、vacuum 失败后再次成功回收、既有 stale 恢复预算及进程入口测试。

成功日志：`final-unit-build.log`、`regression-integration-02.log`、`integration-compile-09.log`、`metastorage-service-integration-02.log`、`pd-tx-integration.log`。相关下游集成源码编译见 `dependent-integration-05.log`。

### 三节点与公式验收

隔离集群：三节点均为 CMG／MetaStorage voter，网络端口 27344–27346、REST 27300–27302、客户端 27800–27802；工作目录为 `runtime/run04`。使用真实网络、Raft、RocksDB／aipersist、三副本八分区表。节点位于同一 JVM，测试独立节点生命周期，未模拟独立宿主机或容器重启。

| 场景 | 实际结果 |
|---|---|
| 基线 | 三节点 Watch marker revision 89，连续 upsert/get 23 次，0 失败 |
| 阻断 MetaStorage ActionRequest／GetLeaderRequest，并停止跟随节点 | 额外保持 15 秒，超过原默认 retryTimeoutMillis=10000；内部 put 仍 pending，没有提前异常完成 |
| 解除阻断／重启跟随节点 | 存活节点 Watch marker 到 329，三节点到 538；读写继续成功，recovered 阶段累计 1516 次 |
| 停止 MetaStorage leader hf6-0 | 新 leader hf6-2；marker 从 619 继续到 802；累计 2307 次，原节点用原目录重启加入 |
| 注入 HOTFIX6_EXPECTED_FATAL_WATCH | 默认 stop 生效，故障节点完成 shutdown；其余节点继续推进，原目录重启后 marker 继续应用；累计 2721 次 |
| 全部关闭后同目录重启 | marker revision 1263；逐行验证 **3783 条**，ID 0–3782 连续、每行 val=id×17、总和 **121612101**，全部一致 |

故障测试包含 4 次被读写夹具记录的瞬时操作异常，均发生在故障／重启阶段；恢复后重新检查进展和最终数据。不能解释为故障期间零错误或零等待。

复用既有 `SelectionClusterProbe.java`（来自 tic-compute-grid 的 sjcgbs-51442 证据），仅调整隔离端口、节点数／副本数、目标节点和当前 marshaller 构造器。计算执行真实 PineVmComputeJob／AnyCookerVM FFM；调用的是 Ignite Compute API，**本轮未启动 data-api HTTP 服务**。

基线、阻断期间、网络恢复后、致命 Watch 节点重启后、全停再启动后共 5 轮：CN／US 的 10→5、500→100、全选 500、零匹配、单股回退、缺失 select 错误均通过；筛选结果与普通计算逐项比较，差异为 0。旧公式样本对应的日期和值保持不变。

证据：`runtime/formula-before-02.log`、`formula-during-fault.log`、`formula-after-network.log`、`formula-after-fatal.log`、`formula-after-full-restart.log`、`cluster-04.log`、`recovery-oracle.log`。公式制品输入哈希在 `runtime/formula-input-hashes.json`。

### 静态检查、失败记录与限制

- 本次改动文件的 26 个 source-set Checkstyle 任务通过（`style-scoped-04.log`）；最后修改的快照测试再次通过完整 MetaStorage integration source-set 检查。
- `git diff --check`、候选补丁 `git apply --reverse --check` 通过。
- 全仓检查不能声明通过：既有 `PartitionSnapshotStorage.java:307` 有 141 字符行超限；全仓任务还会编译此前遗留的 Parquet 诊断测试，因暂存的生产 Parquet 类不在当前代码而失败。未删除这些基线文件、未恢复其暂存功能来绕过检查。
- 初版编译与集成适配失败的日志保留，最终已修正：缺失 import／工厂／事件监听器、旧新服务类型桥接、真实网络地址主机名差异等。
- `runtime/cluster-02.log` 启动卡住来自夹具串行 join；后续改为并行 join。`cluster-03.log` 使用不适用的 SQL UPSERT，读写进展未达标，未计作有效验收。有效场景为 run04 的 RecordView upsert/get。
- 公式首轮缺少 AnyCookerVM jar，日志 `formula-before.log` 保留；补齐真实 native 依赖后重跑成功。
- run04 最终 SQL SUM 被夹具错误地取成 Long，引发 ClassCastException；它未获得整体 PASS。修正夹具后另起 RecoveryOracle，**全停同目录重启并逐行、数量、ID 连续性、总和重新核验通过**。没有删除失败记录或将该次退出码改写为成功。
- 快照共享测试夹具仍使用旧客户端；其桥接层将新 NO_TIMEOUT 转为旧的 -1，避免 Long.MAX_VALUE 加墙钟的溢出；生产 CMG／MetaStorage 直接使用新客户端的饱和 deadline。四种快照组合重跑通过。
- 没有生产首因完整 trace、长时分区压测、跨版本混部／滚动升级验收，也没有证明修复旧网络连接／ReadIndex 服务端锁阻塞问题。
- 无 quorum 的持续故障不会被本次自动“修好”；内部 NO_TIMEOUT 等待组恢复，外部业务超时仍由原有 API／调用方控制。显式配置 noop 的节点不会因默认值变化自动覆盖其配置。

## 与此前 cluster 启动修复的关系

此前 hotfix 处理启动／恢复阶段的 MetaStorage divergence、snapshot／应用索引、LWM 与 stale 恢复预算等问题；本次主要处理**运行期间**元数据请求的等待语义、重试正确性以及 Watch 致命失败后的节点处置，并让 CMG 的启动与运行请求实际接入新客户端。

两者可能表现为“节点在线但请求超时”，但不能互相替代。既有恢复相关代码未整体覆盖；本轮 stale 故障与预算用例继续通过。现场最初 DataNodes 错误仍应与完整 cause 对照，不能仅凭运行期修复把所有启动卡住归为同一问题。

## 交付物与工作区保护

- 可审阅完整候选补丁：`hotfix6.patch`（含新增文件）；源码清单与 SHA-256：`source-manifest.json`。
- 发行包 Ignite JAR 哈希：`package-manifest.json`；最终 XML：`final-test-results/`；计数：`final-test-counts.json`。
- 原 Primary 暂存 `b2decb97adf88c5b37ae85df2d6e2b05e8938fc5`、Parquet 暂存 `c63690e9bf0ff422219c1ebadf222648526ee10f` 均保留。未恢复旧功能、未清理基线未跟踪文件、未覆盖现有 hotfix ZIP。
- 本轮隔离测试进程均已停止，未操作其他运行中的集群或容器。
