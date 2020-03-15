package com.wangyichao.bigdata.flink02

import com.wangyichao.bigdata.flink02.source.{AccessParallelSource, AccessRichParallelSource, AccessSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 三种自定义source的区别
 * 第一种：继承SourceFunction实现自定义source，并行度只能为1
 * 第二种：继承ParallelSourceFunction实现自定义source，可多并行度运行
 * 第三种：继承RichParallelSourceFunction实现自定义source，可多并行度运行，有生命周期函数
 */
object StreamingJobApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * env.addSource(new AccessSource).setParallelism(3).print()
     * 设置并行度为3执行 通过SourceFunction实现的 AccessSource会报错
     * Exception in thread "main" java.lang.IllegalArgumentException: The maximum parallelism of non parallel operator must be 1.
     *
     * 原因源码：DataStreamSource.java中的setParallelism方法
     */
    //第一种：继承SourceFunction实现自定义source，并行度只能为1
    env.addSource(new AccessSource).print()

    //第二种：继承ParallelSourceFunction实现自定义source，可多并行度运行
    env.addSource(new AccessParallelSource).setParallelism(3).print()

    //第三种：继承RichParallelSourceFunction实现自定义source，可多并行度运行，有生命周期函数
    env.addSource(new AccessRichParallelSource).setParallelism(3).print()

    env.execute(this.getClass.getSimpleName)
  }
}
