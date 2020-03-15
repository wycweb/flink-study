package com.wangyichao.bigdata.flink04

import com.wangyichao.bigdata.flink04.partition.AccessPartition
import com.wangyichao.bigdata.flink04.source.AccessRichParallelSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink中的自定义分区器
 */
object StreamingJobApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kvDataStrem = env.addSource(new AccessRichParallelSource)

    kvDataStrem.partitionCustom(new AccessPartition, "domain").map(x => {
      println("current thread id is :" + Thread.currentThread().getId + ", value is :" + x.domain)

      x
    }).print()

    env.execute(this.getClass.getSimpleName)
  }
}
