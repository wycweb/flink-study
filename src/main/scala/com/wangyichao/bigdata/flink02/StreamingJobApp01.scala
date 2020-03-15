package com.wangyichao.bigdata.flink02

import com.wangyichao.bigdata.flink02.bean.Access
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * 官方提到的供debug使用的算子
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html#collection-data-sources
 */
object StreamingJobApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val list = List(Access(20200301,"a.com",1000),Access(20200301,"b.com",2000))

    env.fromCollection(list).print()
    env.fromElements(list).print()

    env.execute(this.getClass.getSimpleName)
  }
}
