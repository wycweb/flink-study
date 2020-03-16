#
Window: 无界流拆分成bucket，然后进行计算

是否keyBy
    Keyed Window    window
    Non-Keyed Window  windowAll

DataStream
    Assigners：window 每条输入的数据如何分发到正确的window

按照时间间隔 Time Window  根据指定的时间处理
    Tumbling Windows ：滚动窗口
        按照固定的窗口长度对数据进行切分

    Sliding Windows ：滑动窗口
        数据是有重叠
        每隔多久统计多久的数据

按照计数窗口 Count Window
    100个元素一个窗口
    和时间没有关系

Window Functions：定义了要对窗口中的数据做什么样的计算

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#window-functions

    增量聚合
        event就进行计算，都有一个state
        The window function can be one of ReduceFunction, AggregateFunction, FoldFunction or ProcessWindowFunction.
        例如：统计5秒中接收到的数据的和
            6 5 11 10
            
            6 ==> 6
            6,5 ==> 11
            6,5,11 ==> 22
            6,5,11,10 ==> 32


    全量聚合
        把窗口中所有的数据准备完毕，等时间到了，在遍历所有数据计算
        可以对窗口中的数据进行排序
        Function：ProcessWindowFunction
        求窗口内的最大值
            6 5 11 10

# Event Time / Processing Time / Ingestion Time
官方文档[各种时间](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_time.html#event-time--processing-time--ingestion-time)

# Flink Watermarks
官方文档[水印的概念](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/event_time.html#event-time-and-watermarks)

# Flink状态管理
State管理以及如何恢复

state : task(并行度)/operator(算子)的管理，内容保存在JVM进程里(TaskManager)

checkpoints：把state的数据持久化

savepoints

官方文档[状态管理](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/state.html)

状态管理有两种：原生态的和托管的
官方文档[状态管理方式](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/state/state.html#raw-and-managed-state)

State分类：有两类

Keyed State
    作用于KeyedStream，每一个key都对应一个state。
    State的数据结构：
        ValueState：T的单一值 支持更新、获取
        ListState：列表
        ReducingState：用户自定义的ReduceFunction传入
        AggregatingState：
        FoldingState：
        MapState：map类型的

Operator State.
    是与key无关的
    有意义的事情：看看Flink Kafka Consumer底层实现原理


State容错

    checkpoint
    默认情况下，flink的checkpoint是关闭的