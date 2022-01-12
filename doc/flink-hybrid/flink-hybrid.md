# flink connector base 源码阅读

## HybridSource
```text
org.apache.flink.connector.base.source.hybrid.HybridSource
  功能 : 基于配置的源链切换底层源的混合源
  继承 : 
  实现 : Source
  属性 : sources  ： 添加的所有数据源列表
  方法 : 
         getBoundedness : 当前数据源是有界的还是无界的
         createReader ： 创建读取器
         createEnumerator ： 创建枚举器
```



## HybridSourceSplitEnumerator
```text
org.apache.flink.connector.base.source.hybrid.HybridSourceSplitEnumerator
  功能 : 包装实际的拆分枚举器并促进源切换。 发生源切换时会延迟创建枚举器以支持运行时位置转换。
  继承 : 
  实现 : SplitEnumerator
  属性 : sources  ： 添加的所有数据源列表
  方法 : 
         start : 开启一个新 source，并调用 source 枚举器进行任务拆分
         handleSplitRequest ： 获取一个 split 请求，然后转发到当前 source 的 handleSplitRequest
         addSplitsBack ： 当发现错误的时候的回调，如果这个 split 是当前 source 的，则设置会当前 source，如果不是，则放入到 pendingSplits 中
         handleSourceEvent ： 处理事件，当 sourceReader 启动后，会发送一个  SourceReaderFinishedEvent 的事件信息回来。
         createEnumerator ： 创建枚举器
         createEnumerator ： 创建枚举器
```


## HybridSourceReader
```text
org.apache.flink.connector.base.source.hybrid.HybridSourceReader
  功能 : 委托给实际源阅读器的混合源阅读器
  继承 : 
  实现 : SplitEnumerator
  属性 : sources  ： 添加的所有数据源列表
  方法 : 
         start : 初始化 或 发送完成事件 SourceReaderFinishedEvent
         pollNext ： 
         snapshotState ： checkpoint , 做通知转发
         handleSourceEvents ： 处理 SwitchSourceEvent（携带 source 信息） 事件，开启一个 sourceReader
         createEnumerator ： 创建枚举器
         createEnumerator ： 创建枚举器
```


## 问题
org.apache.flink.connector.base.source.hybrid.HybridSourceReader.addSplits  这个是由哪里传递下来的？
org.apache.flink.connector.base.source.hybrid.HybridSourceSplitEnumerator.switchEnumerator  这个的 delegatingContext 是怎么用的？


怎么去做数据源的切换？


怎么知道上一个数据源跑完了？



来这里的情况
还习惯么？
做什么事情？
感觉如何？
团队怎么样？

..最近的情况？
儿子谁带？
