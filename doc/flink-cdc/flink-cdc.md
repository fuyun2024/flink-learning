## MySqlTableSourceFactory
```text
com.ververica.cdc.connectors.mysql.table.MySqlTableSourceFactory
功能 ： 用于创建MySqlTableSource配置实例
继承 ：
实现 ： DynamicTableSourceFactory
属性 ： IDENTIFIER 定义了链接类型，flink 规范  
方法 ： createDynamicTableSource ： 创建动态数据源的方法
factoryIdentifier ： 返回链接类型
requiredOptions ： 必须请求参数
optionalOptions ： 可选请求参数
```

## MySqlTableSource
```text
com.ververica.cdc.connectors.mysql.table.MySqlTableSource
功能 ： 描述如何从逻辑描述创建 MySQL binlog 源
继承 ：
实现 ： ScanTableSource
属性 ： host，prot, db, table, serverId, splitSize, fetchSize, enableParallelRead, startupOptions
方法 ： getScanRuntimeProvider ： 创建动态数据源的方法，可以根据参数 enableParallelRead 来走不同的逻辑，并行和单线程
getParallelSourceConf ： 并行度配置文件的构建
```


## MySqlParallelSource
```text
com.ververica.cdc.connectors.mysql.source.MySqlParallelSource
功能 ： MySQL CDC Source 基于FLIP-27和Watermark Signal Algorithm，支持并行读取表快照，然后继续从binlog捕获数据变化。支持并行的，列级checkPoint,无锁
继承 ：
实现 ： Source
属性 ： deserializationSchema, config, startupMode, historyInstanceName
方法 ： getBoundedness ： 返回无界流或者有界流
createReader ： 构建返回 mysqlCDC 数据读取器（创建未完成的阻塞队列，获取配置（conf clone + 这个读取器的 database.server.id），注册监控）
createEnumerator ： 为此源创建一个新的 SplitEnumerator，开始一个新的输入。
restoreEnumerator ： 从检查点恢复枚举器。
getSplitSerializer ： 为源拆分创建序列化程序。 在将拆分从枚举器发送到读取器以及检查点读取器的当前状态时，将对其进行序
```


## MySqlSourceReader
```text
com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader
功能 ： MySQL源拆分的源阅读器。  从记录分辨读取数据、 时间时间的水印处理、数据解析
继承 ： SingleThreadMultiplexSourceReaderBase , SourceReaderBase
实现 ：
属性 ： elementsQueue, splitStates, splitFetcherManager, config, context, SourceReaderOptions, finishedUnackedSplits, subtaskId
方法 ： start ： 发送切分任务请求
initializedState ：
snapshotState ： 返回已经完成切分，但是没有确认的快照
onSplitFinished ： 完成拆分
addSplits ： 添加拆分列表供此读者阅读
handleSourceEvents : 处理SplitEnumerator发送的自定义源事件。
```










# 数据分配源码

## MySqlSplitAssigner
```text
com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner
  功能 : MySqlSplitAssigner负责决定应该处理什么拆分。 它确定拆分处理顺序
  继承 : 
  实现 : 
  属性 : 
  方法 : 
         open ： 调用以打开分配器以获取任何资源，例如线程或网络连接。
         getNext ： 获取下一个分割。
         waitingForFinishedSplits ： 等待完成拆分
         onFinishedSplits ： 用于回调，汇报完成的快照拆分
         addSplits ： 向此分配器添加一组拆分。 例如，当某些拆分处理失败并且需要重新添加拆分时，就会发生这种情况。
         snapshotState : 创建此拆分分配器状态的快照，以存储在检查点中。 # TODO 对快照存储的细节还不是很理解
         notifyCheckpointComplete ： 通知侦听器具有给定checkpointId已完成并已提交。
         close ： 调用以关闭分配器，以防它持有任何资源，如线程或网络连接。
```



## MySqlSnapshotSplitAssigner
```text
com.ververica.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner3
  功能 : 一个MySqlSplitAssigner ，它根据主键范围和块大小将表拆分为小块拆分
  继承 : 
  实现 : MySqlSplitAssigner
  属性 : alreadyProcessedTables（已经处理的表）
         remainingTables（剩余的表）
         assignedSplits（分配的 split）
         remainingSplits（剩余的 split）
         splitFinishedOffsets（快照拆分完成的偏移量） 
         tableFilters（表的过滤）
         chunkSize （拆分的列大小）
         checkpointIdToFinish（要完成的检查点 ID）
  方法 : 
         open ： 调用以打开分配器以获取任何资源，例如线程或网络连接。
         getNext ： 获取下一个分割。首先从剩余的表中获取一个表，并对其进行拆分成多个 split ，放到 remainingSplits 中，然后 递归调用，把 remainingSplits 中的数据返回
         waitingForFinishedSplits ： 等待完成拆分
         onFinishedSplits ： 用于回调，汇报完成的快照拆分
         addSplits ： 向此分配器添加一组拆分。 例如，当某些拆分处理失 败并且需要重新添加拆分时，就会发生这种情况。
         snapshotState : 创建此拆分分配器状态的快照，以存储在检查点中。 # TODO 对快照存储的细节还不是很理解
         notifyCheckpointComplete ： 通知侦听器具有给定checkpointId已完成并已提交。
         close ： 调用以关闭分配器，以防它持有任何资源，如线程或网络连接。
```


## MySqlBinlogSplitAssigner
```text
com.ververica.cdc.connectors.mysql.source.assigners.MySqlBinlogSplitAssigner
  功能 : 一个MySqlSplitAssigner 它只从当前 binlog 位置读取 binlog。
  继承 : 
  实现 : MySqlSplitAssigner
  属性 : jdbc （与 MySQL 服务器一起使用的JdbcConnection扩展）
         tableFilters（表的过滤）
         isBinlogSplitAssigned （是否已经分配 binlogSplit）
  方法 : 
         open ： 链接 jdbc
         getNext ： 创建 binlog 的 split
         waitingForFinishedSplits ： 
         onFinishedSplits ： 
         addSplits ： 
         snapshotState : 
         notifyCheckpointComplete ： 
         close ： 关闭 jdbc
```












## MySqlSplitReader
```text
com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader
  功能 ： MySQL源拆分的源阅读器。从队列中获取 MySqlSplit （自按主键拆分的表） ，然后创建对应的 DebeziumReader ，从中获取迭代器，并包装成一个 MySqlRecords 返回。
  继承 ： 
  实现 ： Source
  属性 ： deserializationSchema, config, startupMode, historyInstanceName
  方法 ： getBoundedness ： 返回无界流或者有界流
          createReader ： 构建返回 mysqlCDC 数据读取器（创建未完成的阻塞队列，获取配置（conf clone + 这个读取器的 database.server.id），注册监控）
          createEnumerator ： 为此源创建一个新的 SplitEnumerator，开始一个新的输入。
          restoreEnumerator ： 从检查点恢复枚举器。
          getSplitSerializer ： 为源拆分创建序列化程序。 在将拆分从枚举器发送到读取器以及检查点读取器的当前状态时，将对其进行序
```


## SnapshotSplitReader
```text
com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader
  功能 ： 一个快照读取器，从表中读取数据的拆分级别，拆分由主键范围分配。
  继承 ： 
  实现 ： DebeziumReader
  属性 ： statefulTaskContext ： 包含 debezium mysql 连接器任务所需条目的有状态任务上下文
          ExecutorService ： 线程服务
          queue : 队列
          currentTaskRunning ： 当前任务是否运行
          splitSnapshotReadTask ： 读取块拆分的任务
          currentSnapshotSplit ： 当前块的 split

  方法 ： submitSplit ： 添加到拆分阅读，这应该只在阅读器空闲时调用。
          pollSplitRecords ： 从 MySQL 读取记录。 该方法在到达拆分结束时应返回null，如果拆分数据正在拉动，则返回空的Iterator 。
          isFinished ： 返回阅读器的当前拆分是否完成。
          close ： 关闭阅读器并释放所有资源。
```

## MySqlSnapshotSplitReadTask
```text
com.ververica.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask
  功能 ： 读取表的快照拆分的任务。
  继承 ： 
  实现 ： DebeziumReader
  属性 ： databaseSchema
          jdbcConnection
          dispatcher 
          snapshotSplit
          offsetContext 
          topicSelector
          snapshotProgressListener 

  方法 ： submitSplit ： 添加到拆分阅读，这应该只在阅读器空闲时调用。
          pollSplitRecords ： 从 MySQL 读取记录。 该方法在到达拆分结束时应返回null，如果拆分数据正在拉动，则返回空的Iterator 。
          isFinished ： 返回阅读器的当前拆分是否完成。
          close ： 关闭阅读器并释放所有资源。
```






## BinlogSplitReader
```text
com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader
  功能 ： Debezium 二进制日志读取器实现，它还支持读取二进制日志并过滤SnapshotSplitReader读取的重叠快照数据
  继承 ： 
  实现 ： Source
  属性 ： deserializationSchema, config, startupMode, historyInstanceName
  方法 ： getBoundedness ： 返回无界流或者有界流
          createReader ： 构建返回 mysqlCDC 数据读取器（创建未完成的阻塞队列，获取配置（conf clone + 这个读取器的 database.server.id），注册监控）
          createEnumerator ： 为此源创建一个新的 SplitEnumerator，开始一个新的输入。
          restoreEnumerator ： 从检查点恢复枚举器。
          getSplitSerializer ： 为源拆分创建序列化程序。 在将拆分从枚举器发送到读取器以及检查点读取器的当前状态时，将对其进行序
```


## MySqlBinlogSplitReadTask
```text
com.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask
  功能 ： 任务读取表的所有 binlog，还支持读取有界（从 lowWatermark 到 highWatermark）binlog
  继承 ： 
  实现 ： Source
  属性 ： deserializationSchema, config, startupMode, historyInstanceName
  方法 ： getBoundedness ： 返回无界流或者有界流
          createReader ： 构建返回 mysqlCDC 数据读取器（创建未完成的阻塞队列，获取配置（conf clone + 这个读取器的 database.server.id），注册监控）
          createEnumerator ： 为此源创建一个新的 SplitEnumerator，开始一个新的输入。
          restoreEnumerator ： 从检查点恢复枚举器。
          getSplitSerializer ： 为源拆分创建序列化程序。 在将拆分从枚举器发送到读取器以及检查点读取器的当前状态时，将对其进行序
```








# 问题

## 什么时候去切分任务的？切分任务的规则是怎么样子？

### 发现数据表的细节？
com.ververica.cdc.connectors.mysql.source.MySqlParallelSource#createEnumerator  在这里去创建 split 分配器（HybridSplit、Binlog 两种模式）
com.ververica.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner#open 在这里去发现 split 、并把它保存到剩余的 split 切分的数组中
com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils#listTables    发现 mysql split 的细节


### 对一张表进行分块的规则细节?
com.ververica.cdc.connectors.mysql.source.assigners.ChunkSplitter  : ChunkSplitter的任务是将表拆分为一组块或称为拆分
com.ververica.cdc.connectors.mysql.source.assigners.ChunkSplitter#generateSplits
com.ververica.cdc.connectors.mysql.source.assigners.ChunkSplitter#splitTableIntoChunks

splitEvenlySizedChunks : 根据拆分列的数字最小值和最大值将表拆分为大小均匀的块，并以chunkSize步长大小翻转块。
splitUnevenlySizedChunks : 通过连续计算下一个块的最大值，将表拆分为大小不均匀的块。(效率较低)





## 数据一致性保证
### chunk 数据一致性保证
在快照读取操作前、后执行 SHOW MASTER STATUS  查询 binlog 文件的当前偏移量，在快照读取完毕后，查询区间内的 binlog 数据并对读取的快照记录进行修正。


### 多个 chunk 数据一致性保证
全量阶段切片数据读取完成后，SplitEnumerator 会下发一个 BinlogSplit 进行增量数据读取。BinlogSplit 读取最重要的属性就是起始偏移量， Flink CDC 增量读取的起始偏移量为所有已完成的全量切片最小的Binlog 偏移量。
当读取数据的时候下发的条件：
1、判断这条记录是不是之前全量处理过的表 && 判断这个记录的 pos 是否与大于之前处理对应表全量时最大 pos ，满足条件下发。
2、判断完成的 split 中是否包含这张表 &&   判断这个记录的 key 是否在之前包全量的 split 区间中，并且 pos 要大于之前的最大值，满足条件下发。




看看 binlog source read 怎么容错恢复的
他每次同步后会更新 split 的信息，然后 sourceRead 快照的时候会存储下来。



输出一篇文章，总结一下进度和遇到的问题
flink source api 的理解
binlog split 会维护每个 快照读的信息，用于判断读取到的数据是否下发。
binlog 运行之后，会定期更新 binlog split 的位置信息，在 Checkpoint 的时候，把数据保存起来。


