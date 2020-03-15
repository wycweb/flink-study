package com.wangyichao.bigdata.flink02

import com.wangyichao.bigdata.flink02.source.MySQLSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 自定义MySQL Source的实现
 * 注意：假如并行度为3，每个分区里面都会有一份MySQL中的数据
 */
object StreamingJobApp03 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new MySQLSource).print()

    env.execute(this.getClass.getSimpleName)
  }
}
