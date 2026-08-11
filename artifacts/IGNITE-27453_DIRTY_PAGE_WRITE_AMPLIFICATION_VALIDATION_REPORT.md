# IGNITE-27453 Dirty Page 写放大对照验证报告

## 1. 验证结论

验证通过。

- 未修复版本在两套全新 3 节点集群中均稳定复现 SafeTime dirty page 持续写放大，复现率为 2/2。
- 当前 IGNITE-27453 回移版本在两套全新 3 节点集群中均未出现持续写放大，修复验证通过率为 2/2。
- 修复后重启节点仍存在一次正常恢复落盘，但此后 checkpoint 连续记录 `Skipping checkpoint (no pages were modified)`，没有继续形成周期性 dirty page/compaction 循环。
- 重新引入 IGNITE-27563、IGNITE-27790 的资源关闭语义后，资源生命周期、SafeTime、快照恢复、3 节点 3 副本写入及离线副本追赶验证均通过。
- 3 节点 AIPersist 写入验证中，Zone 的 `replicas=3`；一个 follower 离线期间继续写入 100 行，原 work 目录重启后，三个节点本地分区均为 110 行，最终复跑的 `lastAppliedIndex/Term` 均为 `116/1`。
- 使用同时包含 IGNITE-27453、IGNITE-27563、IGNITE-27790 语义回移的全新 ZIP 再次执行 3 节点重启回归，结果仍为 `NOT_OBSERVED`：重启节点仅落盘/压缩 705 页，随后连续 4 个 checkpoint 周期无脏页，也未启动新的 compaction round。
- 当前同名 `ignite3-3.1.0-hotfix1.zip` 早于 IGNITE-27563、IGNITE-27790 重新回移，不能作为本次源码的交付包；本轮验证使用的是重新构建且已记录 SHA-256 的标准 ZIP。

## 2. 对照版本

| 版本 | 源码状态 | 说明 |
| --- | --- | --- |
| 未修复基线 | `04bb4bb7fdd6558b6ae2df2e81e7bb378b93e686`，源码 diff 为空 | 包含 IGNITE-27405，不包含 IGNITE-27453 |
| 当前修复版 | 同一 HEAD 叠加当前工作区 IGNITE-27453、IGNITE-27563、IGNITE-27790 语义回移 | Zone 级 SafeTime 只推进一次；colocation 表级 `PartitionListener` 不再持有/推进 SafeTime tracker；分区 restart/节点 stop 正确关闭并移除 Zone 资源 |

基线构建清单：`build/ignite-27453-ab/baseline/artifacts/build-manifest.env`。

首次修复版构建清单：`build/ignite-27453-ab/fixed/artifacts/build-manifest.env`。

重新引入 IGNITE-27563、IGNITE-27790 后的构建清单：`build/ignite-27453-final/fixed-27563-27790/artifacts/build-manifest.env`。

## 3. 场景与判定标准

### 3.1 集群和负载

- JDK：25.0.2
- Ignite：3.1.0，zone based replication 开启
- 节点数：3
- Zone：1 个 aipersist Zone
- 表数：15
- 每表分区数：32
- 副本数：3
- 理论表分区文件数：`15 × 32 = 480`
- SafeTime 推进间隔：100 ms
- checkpoint 间隔：10 s，deviation 设为 0
- 故障操作：停止 node3 30 s，使用原 work 目录重启，观察 60 s
- 每轮均重建空白 runtime 和集群，不复用上一轮数据目录

### 3.2 判定标准

`files` 表示 compactor 本轮扫描/选择的 delta 文件数，不能单独证明所有文件都是 SafeTime 写脏。因此最终判据同时使用 checkpoint 页数和持续性：

- 大批量写阈值：`480 × 80% = 384 pages`。
- 在重启观察窗内，重启节点出现至少 3 次 `pages >= 384` 的 checkpoint，判定持续写放大已复现。
- 只有一次启动追赶写入，之后连续出现 `Skipping checkpoint (no pages were modified)`，判定没有持续写放大。

## 4. 执行流程

1. 从未修复 worktree 构建官方 DB + CLI ZIP，记录 Git HEAD、源码 diff SHA 和 ZIP SHA。
2. 创建全新 3 节点集群，创建 15 表 × 32 分区 × 3 副本。
3. 等待初始 DDL/seed 写入收敛，采集 idle baseline。
4. 停止 node3 30 秒，使 SafeTime Raft 命令积累。
5. 使用原 work 目录重启 node3，采集 60 秒 checkpoint、compaction 和 CPU。
6. 删除运行数据、重建全新集群，再执行一轮；基线共两轮。
7. 使用当前修复源码重新构建 ZIP，以相同配置执行两套全新集群；修复版共两轮。
8. 执行相关单元/集成测试、发行包构建、ZIP 完整性和包内关键 JAR 校验。

复现脚本：`scripts/job-classloader-cache/07-reproduce-safetime-dirty-page-write-amplification.sh`。

隔离执行脚本：`scripts/job-classloader-cache/08-run-safetime-dirty-page-validation.sh`。

## 5. A/B 结果

以下数据均为重启节点 node3 的 restart 观察阶段：

| 版本/轮次 | checkpoint 次数 | checkpoint 页数 | 大批量 checkpoint 次数 | 无脏页跳过 | 最大 compaction files | compaction 页数 | 平均 CPU | 峰值 CPU | 判定 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 未修复 final1 | 6 | 3150 | 6 | 0 | 480 | 2724 | 29.4% | 192.6% | OBSERVED |
| 未修复 final2 | 5 | 2730 | 5 | 0 | 480 | 2250 | 32.5% | 196.2% | OBSERVED |
| 修复后 final2 | 1 | 495 | 1 | 4 | 165 | 495 | 21.3% | 341.1% | NOT_OBSERVED |
| 修复后 final3 | 1 | 495 | 1 | 4 | 165 | 495 | 20.1% | 292.2% | NOT_OBSERVED |
| 修复后 final4-long（180 秒） | 1 | 495 | 1 | 16 | 165 | 495 | 6.6% | 317.7% | NOT_OBSERVED |
| 修复后 fixed-27563-27790 | 1 | 705 | 1 | 4 | 270 | 705 | 21.0% | 154.3% | NOT_OBSERVED |

说明：修复版 CPU 峰值发生在 node3 JVM 启动/恢复瞬间，不具有持续性；平均 CPU 降低，且后续 4 个 checkpoint 周期均无 dirty page。生产问题的关键特征是持续写入，而不是单次启动峰值。

两轮平均值对比：

- 重启节点 checkpoint 页数：未修复 2940，修复后 495，下降约 83.2%。
- 重启节点 compaction 页数：未修复 2487，修复后 495，下降约 80.1%。
- 最大 compaction 文件数：480 降为 165，下降约 65.6%。
- 未修复版每 10 秒继续写约 480 页；修复版完成一次恢复写入后不再产生新 dirty page。

原始结果：

- `build/ignite-27453-ab/baseline/results/safetime-dirty-page/final1/`
- `build/ignite-27453-ab/baseline/results/safetime-dirty-page/final2/`
- `build/ignite-27453-final/fixed/results/safetime-dirty-page/final2/`
- `build/ignite-27453-final/fixed/results/safetime-dirty-page/final3/`
- `build/ignite-27453-final/fixed/results/safetime-dirty-page/final4-long/`
- `build/ignite-27453-final/fixed-27563-27790/results/safetime-dirty-page/fixed-27563-27790/`

### 5.1 Dirty page 收敛后 Compactor 是否继续循环

这是本次验证的独立验收项。结论是：**SafeTime 导致的持续 compactor 循环已经解决；恢复阶段允许有限的 1～2 轮 compaction，但 dirty page 收敛后没有继续启动新的 compaction round。**

#### 未修复版本：持续产生 dirty page，并持续触发 compactor

node3 在 60 秒观察窗内每约 10 秒产生一批新 dirty page，随后立即启动新的 480-files compaction：

```text
00:27:26 Checkpoint started pages=810
00:27:28 Starting new compaction round files=480
00:27:31 Compaction finished pages=810
00:27:36 Checkpoint started pages=480
00:27:40 Starting new compaction round files=480
00:27:43 Compaction finished pages=480
00:27:46 Checkpoint started pages=480
00:27:53 Starting new compaction round files=480
00:27:55 Compaction finished pages=480
00:27:57 Checkpoint started pages=480
00:27:59 Starting new compaction round files=480
00:28:01 Compaction finished pages=480
00:28:07 Checkpoint started pages=480
00:28:09 Starting new compaction round files=480
```

这个序列没有出现 `Skipping checkpoint (no pages were modified)`，说明不是 compactor 单纯排历史积压，而是 SafeTime 仍在周期性制造新的 dirty page。

原始日志：`build/ignite-27453-ab/baseline/results/safetime-dirty-page/final2/restart-node3.log`。

#### 修复版本：重启节点只执行一轮 compaction，之后连续 16 个周期无 dirty page、无新 compaction

```text
09:41:42.533 Checkpoint started pages=495
09:41:42.877 Starting new compaction round files=165
09:41:43.395 Compaction round finished pages=495 duration=517ms
09:41:52.531 Skipping checkpoint (no pages were modified)
09:42:02.532 Skipping checkpoint (no pages were modified)
09:42:12.544 Skipping checkpoint (no pages were modified)
09:42:22.541 Skipping checkpoint (no pages were modified)
09:42:32.546 Skipping checkpoint (no pages were modified)
09:42:42.546 Skipping checkpoint (no pages were modified)
09:42:52.552 Skipping checkpoint (no pages were modified)
09:43:02.551 Skipping checkpoint (no pages were modified)
09:43:12.563 Skipping checkpoint (no pages were modified)
09:43:22.562 Skipping checkpoint (no pages were modified)
09:43:32.564 Skipping checkpoint (no pages were modified)
09:43:42.573 Skipping checkpoint (no pages were modified)
09:43:52.581 Skipping checkpoint (no pages were modified)
09:44:02.583 Skipping checkpoint (no pages were modified)
09:44:12.594 Skipping checkpoint (no pages were modified)
09:44:22.598 Skipping checkpoint (no pages were modified)
```

从 `09:41:43.395` 第一轮 compaction 结束，到 `09:44:22.598` 观察结束：

- 后续 checkpoint 检查次数：16。
- 后续实际 checkpoint 次数：0。
- 后续新增 dirty pages：0。
- 后续 `Starting new compaction round`：0。
- 无 compactor 时间：159.203 秒。

原始日志：`build/ignite-27453-final/fixed/results/safetime-dirty-page/final4-long/restart-node3.log`。

#### 另外两个存活节点：两轮有限恢复写入后停止

node1 和 node2 各出现两轮有限写入：第一轮 495 pages，第二轮 330 pages；第二轮 compaction 均在 `09:41:37` 左右完成。此后直到 `09:44:25`，每个节点连续 17 个 checkpoint 周期均无 dirty page，且没有第三轮 compaction。

这说明修复目标不是“集群恢复期间绝对不运行 compactor”，而是“停止 SafeTime 周期性制造新 dirty page，使 compactor 在有限恢复/积压排空后真正停止”。

原始日志：

- `build/ignite-27453-final/fixed/results/safetime-dirty-page/final4-long/restart-node1.log`
- `build/ignite-27453-final/fixed/results/safetime-dirty-page/final4-long/restart-node2.log`

#### 结论边界

- 已解决：SafeTime 每次推进都把全部表分区写脏，导致 checkpoint 和 compactor 无限循环。
- 仍属正常：节点重启/副本恢复产生的有限 dirty page，以及对已有 delta 文件的有限 compaction。
- 如果在已经受影响的生产 work 目录上直接升级，修复不会删除升级前已经生成的 delta 文件；compactor 可能继续运行若干轮排完历史积压。但只要后续 checkpoint 开始持续出现 `Skipping checkpoint (no pages were modified)`，且 compaction 文件/页数单调归零，这个过程会结束，不是问题复发。

## 6. 源码修复为何有效

未修复时，Zone 级 `SafeTimeSyncCommand` 被分发给同一 ZonePartition 下的所有表级 processor。每个表级 `PartitionListener` 都推进自己的 SafeTime 和 last-applied 存储元数据。15 张表 × 32 个分区会把一次逻辑 SafeTime 推进放大为约 480 份表分区持久化更新；周期性 SafeTime 命令不断制造 dirty page，checkpoint 又持续生成 delta 文件，随后 compactor 持续合并这些文件。

当前回移实现：

- `ZonePartitionRaftListener` 在 ZonePartition 层直接处理 `SafeTimeSyncCommand`，只推进一次 Zone 级 tx state storage 的 last-applied。
- colocation 模式下，`TableManager` 创建表级 `PartitionListener` 时不再传入 SafeTime tracker。
- 表级 `PartitionListener` 只有在 tracker 非空的 legacy non-colocation 模式下才处理 SafeTime 命令和推进 tracker。
- 相关测试夹具已同步使用 Zone/no-tracker 构造路径，避免测试环境继续模拟错误的表级 SafeTime ownership。

因此修复消除了“一个 Zone SafeTime 命令 × Zone 内所有表”的持久化写放大，同时保留 non-colocation 兼容路径。

## 7. 自动化测试与构建

以下验证均通过：

- `:ignite-table:compileTestFixturesJava`
- `:ignite-table:compileIntegrationTestJava`
- `:ignite-partition-replicator:compileIntegrationTestJava`
- `ItZonePartitionRaftListenerRecoveryTest`
- `ItTxObservableTimePropagationTest`（colocation 默认模式）
- `ItTxObservableTimePropagationTest -DIGNITE_ZONE_BASED_REPLICATION=false --rerun-tasks`（legacy non-colocation）
- changed-area compute 单元测试
- `:packaging:clean :packaging-cli:clean :packaging-db:clean :packaging:allDistZip -PprojectVersion=3.1.0`
- `git diff --check`
- hotfix ZIP `unzip -tq` 完整性校验

### 7.1 IGNITE-27563、IGNITE-27790 重新回移验证（2026-08-11）

重新回移后的源码语义：

- `SafeTimeValuesTracker` 与 `storageIndexTracker` 由分区生命周期创建，并显式交给 `ZoneResourcesManager`，资源所有权清晰。
- 分区 restart 时先关闭 tracker；仅当 replica 确实停止后才从 Zone 资源映射移除，tx-state partition storage 保留供重启复用。
- 节点停止时关闭 Zone 下所有 tracker 和 tx-state partition storage，并清空 partition resource map。
- 以上行为没有改变 IGNITE-27453 的 Zone 级 SafeTime ownership，也没有重新引入表级 SafeTime 推进。

执行结果：

| 验证命令/测试 | 数量 | 结果 |
| --- | ---: | --- |
| `PartitionReplicaLifecycleManagerTest` + `ZoneResourcesManagerTest` | 11 | 全部通过 |
| `ZonePartitionRaftListenerTest` | 29 | 全部通过 |
| `ItZonePartitionRaftListenerRecoveryTest` | 5 | 全部通过 |
| `ItTxObservableTimePropagationTest` | 1 | 通过 |
| partition-replicator main/test 与 table integrationTest Checkstyle | 3 tasks | 全部通过 |

关键覆盖项包括：分区 restart 关闭 tracker、manager stop 关闭 tracker/tx-state storage、异常停止、Zone 资源 shutdown、Zone 级 `SafeTimeSyncCommand`、快照恢复、last-applied 更新、leader 切换后的 SafeTime/observable time 单调传播。

### 7.2 3 节点、Zone replicas=3 的 AIPersist 写入同步验证

新增测试：

`modules/table/src/integrationTest/java/org/apache/ignite/distributed/ItAipersistThreeReplicaDataSyncTest.java`

测试配置和步骤：

1. 启动 3 个节点；CMG、MetaStorage 均使用 3 副本，确保停止任意一个节点仍保有 2/3 多数派。
2. 显式创建 `default_aipersist` Zone，`partitions=1`、`replicas=3`；在三个节点分别从 Catalog 断言 `zone.replicas()==3`，并确认三个节点都存在本地 partition storage。
3. 三节点在线时写入 10 行，并直接扫描三个节点的本地 `MvPartitionStorage`，确认各自均为 10 行。
4. 通过 Placement Driver 找到非 primary 的 follower 并停止，本次为 `iatrdst_wartatarafr_13344`。
5. 使用剩余两个节点的 Raft 多数派继续提交 100 行，总行数达到 110。
6. 不清理 work 目录，原目录重启 follower；等待三个本地 AIPersist 分区均追平到 110 行。
7. 逐节点记录本地行数和 `MvPartitionStorage.lastAppliedIndex/Term`，不能用可能路由到 primary 的 SQL 查询代替。

执行命令：

```text
./gradlew :ignite-table:integrationTest --tests '*ItAipersistThreeReplicaDataSyncTest' --console=plain
```

结果：`BUILD SUCCESSFUL`，最终复跑测试 1/1 通过，JUnit 记录耗时 16.198 秒。关键日志：

```text
DATA_SYNC_REPLICA node=iatrdst_wartatarafr_13344 rows=110 lastAppliedIndex=116 lastAppliedTerm=1
DATA_SYNC_REPLICA node=iatrdst_wartatarafr_13345 rows=110 lastAppliedIndex=116 lastAppliedTerm=1
DATA_SYNC_REPLICA node=iatrdst_wartatarafr_13346 rows=110 lastAppliedIndex=116 lastAppliedTerm=1
DATA_SYNC_RESULT=PASS nodes=3 replicas=3 rows=110 restartedFollower=iatrdst_wartatarafr_13345
```

原始日志和 JUnit XML 已固化到 `build/ignite-27453-final/fixed-27563-27790/results/data-sync/`。日志 SHA-256 为 `7d75e700f6461607462a60ec5f12757729147a7d878a16fba5584a8fee7a17cc`，JUnit XML SHA-256 为 `09fefcce21137ad9c2d434ad423bfcffceaaeede0ca25f30521718327266225f`。测试结论：客户端写入提交、Raft 多数派复制、离线 follower 原目录重启后的日志追赶以及三个 AIPersist 本地副本数据一致性均正常；IGNITE-27563、IGNITE-27790 的资源关闭语义没有破坏数据同步。

### 7.3 重新回移后的 dirty page/compactor 独立集群回归

本轮使用当前工作区重新构建标准发行 ZIP，并在独立目录、独立端口创建全新 3 节点集群；没有复用历史 runtime。构建指纹如下：

```text
GIT_HEAD=04bb4bb7fdd6558b6ae2df2e81e7bb378b93e686
SOURCE_DIFF_SHA256=118286895f9ed29108dc0eba11d2ccc7bdd0cbb448367e458da9b5149b382f6c
ZIP_SHA256=fe959976c5c327a80651a614789c95eac8e5726e8694edb447fa50d11ca65002
JAVA_MAJOR=25
```

场景保持与 A/B 验证一致：15 张 AIPersist 表、每表 32 分区、3 副本，SafeTime 间隔 100 ms，checkpoint 间隔 10 秒；停止 node3 30 秒后使用原 work 目录重启并观察 60 秒。

重启节点 node3 的汇总结果：

```text
checkpoint_count=1
checkpoint_pages=705
large_checkpoint_count=1
skipped_checkpoint_count=5
too_many_dirty_count=0
compaction_start_count=1
max_compaction_files=270
compaction_finish_count=1
compaction_pages=705
SAFETIME_DIRTY_PAGE_WRITE_AMPLIFICATION=NOT_OBSERVED
```

其中 `skipped_checkpoint_count=5` 包含节点停止阶段的一次跳过。恢复落盘后 60 秒观察窗内的关键时序为：

```text
11:57:40 Checkpoint started pages=705 reason='timeout'
11:57:41 Starting new compaction round files=270
11:57:42 Compaction round finished pages=705 duration=1010ms
11:57:50 Skipping checkpoint (no pages were modified)
11:58:00 Skipping checkpoint (no pages were modified)
11:58:10 Skipping checkpoint (no pages were modified)
11:58:20 Skipping checkpoint (no pages were modified)
```

该观察窗内 `reason='too many dirty pages'` 次数为 0；恢复阶段只有一轮正常落盘和 compaction，之后连续 4 个周期无 dirty page、无第二轮 compaction。由此确认重新引入 IGNITE-27563、IGNITE-27790 的资源关闭语义没有破坏 IGNITE-27453 对持续写放大的修复。

两个存活节点也完成了收敛：node1、node2 在 node3 离线和重新加入引起的拓扑/恢复写入期间各累计 3 轮 compaction（330、330、540 页），最后一轮分别于 11:57:34、11:57:35 完成；此后两节点均连续 5 个 checkpoint 周期记录 `Skipping checkpoint (no pages were modified)`，没有第四轮 compaction，也没有 `too many dirty pages`。因此本轮不是只验证重启节点，原问题中会持续高 CPU/写盘的其余副本节点也已验证停止循环。

原始证据：

- `build/ignite-27453-final/fixed-27563-27790/artifacts/build-manifest.env`
- `build/ignite-27453-final/fixed-27563-27790/results/safetime-dirty-page/fixed-27563-27790/summary.csv`
- `build/ignite-27453-final/fixed-27563-27790/results/safetime-dirty-page/fixed-27563-27790/restart-node3.log`
- `build/ignite-27453-final/fixed-27563-27790/logs/07-safetime-dirty-page-reproduction-fixed-27563-27790.log`

## 8. Hotfix1 交付物

> 注意：以下 ZIP 是引入 IGNITE-27563、IGNITE-27790 语义回移之前构建的历史产物。本节哈希只用于追溯，不能代表当前工作区源码；如果要发布包含这两项回移的新包，必须重新构建并更新哈希。

- 文件：`build/hotfix-distributions/ignite3-3.1.0-hotfix1.zip`
- 大小：246085190 bytes
- 生成时间：2026-08-11 01:01:55 +0800
- SHA-256：`7a3b5280dec85e791a993957062353205344c49938bb4dc668b87c5521cc5f5d`
- 校验文件：`build/hotfix-distributions/ignite3-3.1.0-hotfix1.zip.sha256`

包内校验：

- 包内 `ignite-partition-replicator-3.1.0.jar` 与本次源码构建 JAR SHA-256 一致：`2823363ae06d54d1568128e55435d3c819f70ac355b71e0d8c070f26072be736`。
- 包内 `ignite-table-3.1.0.jar` 与本次源码构建 JAR SHA-256 一致：`39b7cb484772ed830c2e754e52b08aa8db2b994d4447492dc893283910df2e48`。
- `javap` 确认包内 `ZonePartitionRaftListener` 包含 Zone 级 `handleSafeTimeSyncCommand(long, long)`。
- `javap` 确认包内 `PartitionListener` 同时包含 colocation 无 SafeTime tracker 构造函数和 legacy tracker 构造函数。

旧的同名包已保留为 `ignite3-3.1.0-hotfix1.pre-IGNITE-27453.20260810.zip`，未覆盖丢失。

## 9. 最终判断

当前 IGNITE-27453 回移满足本问题的修复目标：节点重启后允许有限的 1～2 轮 Raft/存储恢复落盘或历史 delta 排空，但不会再因为 SafeTime 周期推进而反复将全部表分区元数据写脏。180 秒加速观察覆盖 18 个 checkpoint 周期，重启节点在第一轮 compaction 后连续 16 个周期无 dirty page、无新 compaction；其他两个节点在第二轮 compaction 后连续 17 个周期无 dirty page、无第三轮 compaction。重新引入 IGNITE-27563、IGNITE-27790 后再次执行的独立集群回归同样只有一轮 705 页恢复 compaction，之后连续 4 个周期无 dirty page，且 3 副本 AIPersist 写入与离线 follower 追赶一致。持续 checkpoint/compaction 循环已经消除，资源关闭语义没有影响数据同步。
