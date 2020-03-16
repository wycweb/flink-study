# Flink 流中的Souce
文档链接：[Data Sources](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html#data-sources)

# DataSource中的几种方式
| 类                         | 备注                     | 测试代码                  |
| :-----                     | ----:                    | :----:                    |
| SourceFunction             | 并行度只能为1            | AccessSource.scala        |
| ParallelSourceFunction     | 多并行度                 | AccessParallelSource      |
| RichParallelSourceFunction | 多并行，有生命周期函数   | AccessRichParallelSource  |

# 对接Kafka

注意版本问题：
文档链接：[Kafka Sources](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/connectors/kafka.html#apache-kafka-connector)

# DataStream 转换算子
文档链接：[DataStream Transformations](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/index.html#datastream-transformations)

## 转换算子：分流的详细操作
[flink如何正确分流](https://www.jianshu.com/p/274d0b78d378)

## 转换算子：合流
合流有两个算子：Union和Connect

区别：

Union合流时，可以合并多个数据流，但是两个DataStream的结构必修是一致的结构

Connect合流时，连接两个数据流，数据DS的结构可以不同，Connect生成的结构是ConnectedStreams[IN,OUT]，使用时需要按需转换成正常的DataStream结构

## 分区器
https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/index.html#physical-partitioning


## 目录
StreamingJobApp01 debug过程用的几个算子

StreamingJobApp02 三种自定义source的方式

StreamingJobApp03 自定义MySQLSource的实现

StreamingJobApp04 Flink对接kafka的demo