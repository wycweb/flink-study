# Flink 流中的Sink
文档链接：[Data Sinks](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html#data-sinks)

# Flink中的buffer
严格意义来讲，flink中的数据并非一条一条的，flink中有buffer的概念.

默认情况元素不会逐个传输，而是进入缓冲区。

缓冲区默认值为100ms，提高该值能增大吞吐量，但也可能带来数据延迟问题。设置小了，吞吐量会下降。

为了增大吞吐量，setBufferTimeout(-1)，只有缓冲区满了才会发送数据。

为了降低延迟，设置为接近0的值(如5ms或10ms)，但是不要设置为0，设置为0可能会导致性能下降。

https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html#controlling-latency

## 目录
StreamingJobApp01 自定义MySQL sink实现