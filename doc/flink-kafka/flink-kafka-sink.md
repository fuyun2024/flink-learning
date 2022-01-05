# flink kafka sink 源码阅读

## FlinkKafkaProducer
```text
org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
  功能 : Flink Sink 将数据生成到 Kafka 主题中。 默认生产者将使用FlinkKafkaProducer.Semantic.AT_LEAST_ONCE语义。 在使用FlinkKafkaProducer.Semantic.EXACTLY_ONCE之前，请参考 Flink 的 Kafka 连接器文档。
  继承 : TwoPhaseCommitSinkFunction
  实现 : 
  属性 : Semantic[EXACTLY_ONCE、AT_LEAST_ONCE、NONE]  ： 一致性等级
        NEXT_TRANSACTIONAL_ID_HINT_DESCRIPTOR_V2 ： 事务 ID 列表的描述符 state
        nextTransactionalIdHintState : nextTransactionalIdHint 的状态。
        producerConfig : 配置
        keyedSchema ： 
        kafkaSchema ： 
        flinkKafkaPartitioner ： 分区器，决定怎么对 kafka  进行分区的
        topicPartitionsMap ： topic 和 分区的映射关系
        kafkaProducersPoolSize ： 池中生产者的最大数量
        availableTransactionalIds ： 可用事务 ID  队列
        writeTimestampToKafka : 延迟控制我们是否将 Flink 记录的时间戳写入 Kafka
        logFailuresOnly : 指示是否接受失败（并记录它们）或失败时失败的标志。
        callback : 回调比处理错误传播或日志回调
        asyncException : 在异步生产者中遇到的错误存储在这里。
        previouslyCreatedMetrics ： 缓存指标以替换已注册的指标，而不是覆盖现有指标。
        transactionalId # todo
  方法 : 
         FlinkKafkaProducer : [defaultTopic、keyedSchema、customPartitioner、kafkaSchema、producerConfig、semantic、kafkaProducersPoolSize]
         open ： 调用以打开分配器以获取任何资源，例如线程或网络连接。
         getNext ： 获取下一个分割。
         waitingForFinishedSplits ： 等待完成拆分
         onFinishedSplits ： 用于回调，汇报完成的快照拆分
         addSplits ： 向此分配器添加一组拆分。 例如，当某些拆分处理失败并且需要重新添加拆分时，就会发生这种情况。
         snapshotState : 创建此拆分分配器状态的快照，以存储在检查点中。 # TODO 对快照存储的细节还不是很理解
         notifyCheckpointComplete ： 通知侦听器具有给定checkpointId已完成并已提交。
         close ： 调用以关闭分配器，以防它持有任何资源，如线程或网络连接。
         
         t
         
```