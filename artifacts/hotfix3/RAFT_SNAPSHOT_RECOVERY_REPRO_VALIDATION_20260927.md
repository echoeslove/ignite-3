# Ignite 3.1 Raft 快照中断后的节点恢复：复现与修复验证（2026-09-27）

## 范围与版本

- 基线：本仓库 `hotfix/hotfix-3`，HEAD `b55e93a130`，复现前不含 IGNITE-26849 / IGNITE-27097。
- 官方修复来源：[IGNITE-26849](https://issues.apache.org/jira/browse/IGNITE-26849)，合并提交 `f6eaf1254bebef6514ec976e6aa8e307eee801e4`；[IGNITE-27097](https://issues.apache.org/jira/browse/IGNITE-27097)，合并提交 `db73d7063439b130b15f320b5cdd38ceefa4bfe1`。两个问题的官方修复版本均为 3.2。
- 本次仅回移源码和测试，未重新打包或部署集群。测试环境为本机三节点、一分区、三副本，默认持久化存储和 zone based replication。Gradle 8.6 用 JDK 21 运行。

重建补丁前基线时，从 `b55e93a130` 建立独立检出，仅移入本报告提及的两个测试类和 `modules/raft/build.gradle` 的测试依赖变更，再执行下列命令；不要移入生产代码补丁。本次实测基线就是这样的生产代码状态。

## 复现一：日志截断后中断快照安装

测试入口：`ItTableRaftSnapshotsTest.testRestartAfterTruncatedLogAndInterruptedSnapshotInstall`。

1. 启动三节点集群，建立一分区、三副本的表；将分区 Raft leader 转到 node0。
2. 写入键 1、2；在三个副本上强制创建 Raft 快照，使日志前缀被截断。
3. 在 node0 拦截发往 node2 的 `SnapshotMvDataResponse`，停掉 node2。
4. 在 node0 再写入键 3、4，并再次强制创建快照。恢复 node2，确认它发起了快照安装且 MV 数据响应被拦截。
5. 快照安装尚未完成时再次停掉 node2，解除消息拦截，随后重启 node2。
6. 断言 node2 成功启动，且键 1～4 的值分别为 `one`、`two`、`three`、`four`。

复现命令：

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :ignite-raft:integrationTest \
  --tests 'org.apache.ignite.internal.raftsnapshot.ItTableRaftSnapshotsTest.testRestartAfterTruncatedLogAndInterruptedSnapshotInstall' \
  --no-daemon --console=plain
```

补丁前实测：1 个用例失败，第二次启动 node2 时抛出 `NodeStartException`。`modules/raft/build/ignite.log` 在失败轮次记录：

```text
Node <20_part_0/itrst_tratlaisi_3346> received InstallSnapshotRequest ... lastIncludedLogIndex=45
Node <20_part_0/itrst_tratlaisi_3346> is initialized with inconsistent log,
status=Status[EIO<1014>: Missing logs in (0, 12)].
```

补丁后实测：同一用例通过，node2 成功重启并读到 4 条预期数据；最终联合回归轮次耗时 20.556 秒，0 失败。

## 复现二：持久化存储仅有部分 rebalance 标记

测试入口：`ItInterruptedRaftSnapshotStorageRecoveryTest`。这是官方 IGNITE-27097 的两个用例，移植时仅适配了 3.1 的测试 API。

1. 启动三节点、一分区、三副本集群，在显式事务中写入一行，并确认三个节点均有该行。
2. 对三个副本强制创建快照并截断日志前缀，停止 node2。
3. 在 node2 已停止的持久化目录中，分别模拟两种中断状态：MV 分区存储处于 `REBALANCE_IN_PROGRESS`；或者 TX state 分区存储处于 `REBALANCE_IN_PROGRESS`。将标记刷盘。
4. 重启 node2，断言可恢复，并重新读到该行。

复现命令：

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :ignite-raft:integrationTest \
  --tests 'org.apache.ignite.internal.raftsnapshot.ItInterruptedRaftSnapshotStorageRecoveryTest' \
  --no-daemon --console=plain
```

仅应用 IGNITE-26849 时：MV 分区场景通过，TX state 分区场景失败；node2 启动时报 `ZonePartitionKey [zoneId=20, partitionId=0]: empty configuration in startup snapshot`。因此本次还回移了 IGNITE-27097。

两个补丁应用后：上述两个用例均通过；联合回归连续两轮分别耗时 28.117 秒、27.004 秒，均为 0 失败。

## 修改机制与 3.1 适配

- IGNITE-26849：快照接收端开始 rebalance 时同步清理对应的 Raft 日志/元数据；节点启动检测到未完成的 MV rebalance 时，也清理 Raft 协议存储，避免数据存储重置后仍留下已截断的旧日志。
- IGNITE-27097：在启动副本前统一收集 MV 与 TX state 的 rebalance 状态；只要任一存储处于中断状态，就先清理 Raft 协议存储，再清理 TX/MV 存储，最后启动副本。顺序用于保证再次断电后仍可识别并重做清理。
- 3.1 仍保留非 colocation 模式；回移时保留其表创建监听、表分区存储恢复和 Raft 存储销毁路径。`PartitionSnapshotStorage` 继续接受通用 `PartitionKey`，而不是 3.2 中仅接受 `ZonePartitionKey`。
- 本次没有更改只读副本路由、Primary 选主或自动 rebalance 策略。

## 最终验证

| 测试 | 结果 |
| --- | --- |
| 截断日志后中断快照安装并两次重启 node2 | 联合回归两轮均 1/1 通过；第二轮 21.814 秒 |
| MV 与 TX state 持久化中断状态恢复 | 联合回归两轮均 2/2 通过；第二轮 27.004 秒 |
| `PartitionReplicaLifecycleManagerTest` | 8/8 通过 |
| `IncomingSnapshotCopierTest` | 7/7 通过 |
| `ZoneResourcesManagerTest`、`PartitionSnapshotStorageTest`、`PartitionSnapshotStorageFactoryTest` | 13/13 通过 |
| `ItZonePartitionRaftListenerRecoveryTest` | 5/5 通过 |
| `TableManagerTest`，包含非 colocation 用例 | 14/14 通过 |
| `ItReadOnlyLocalReplicaNetworkTest.localViewsReadConsistentSnapshotsWithoutRemoteRowReadsDuringWrites`，持续写入期间本地只读副本查询 | 1/1 通过，19.802 秒；使用仓库中已有的未跟踪测试文件，仅作回归验证 |
| `git diff --check HEAD` | 通过 |

最终联合回归命令：

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
  ./gradlew :ignite-raft:integrationTest \
  --tests 'org.apache.ignite.internal.raftsnapshot.ItInterruptedRaftSnapshotStorageRecoveryTest' \
  --tests 'org.apache.ignite.internal.raftsnapshot.ItTableRaftSnapshotsTest.testRestartAfterTruncatedLogAndInterruptedSnapshotInstall' \
  --no-daemon --console=plain
```

Gradle XML 结果位于 `modules/raft/build/test-results/integrationTest/`、`modules/partition-replicator/build/test-results/test/`、`modules/partition-replicator/build/test-results/integrationTest/` 和 `modules/table/build/test-results/test/`；同一任务下一轮运行可能覆盖上一轮的 XML。最后一轮 Raft 集成测试的 `modules/raft/build/ignite.log` 未发现 `Missing logs in`、`inconsistent log` 或 `empty configuration in startup snapshot`。

## 发布前仍需完成

按现有 hotfix3 发布流程重新构建候选包、核对包内代码版本，并在独立测试集群做一次滚动重启与中断快照恢复验收。本报告证明上述本地复现路径已经修复，不代表现有线上 node2 的持久化目录一定可以直接恢复；升级前应保留故障节点目录及健康副本的备份。
