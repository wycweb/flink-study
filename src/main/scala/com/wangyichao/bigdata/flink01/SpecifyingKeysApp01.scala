package com.wangyichao.bigdata.flink01

import com.wangyichao.bigdata.flink01.bean.{WC, WorldCount}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object SpecifyingKeysApp01 {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收数据
    //val text: DataStream[String] = env.socketTextStream("localhost", 7777)
    val text = env.readTextFile("data/wc.data")


    val splitValue = text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)

    //https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/api_concepts.html#specifying-keys
    //元祖方式
    val reult1 = splitValue.map((_, 1)).keyBy(0).sum(1)

    //java的POJO
    val result2 = splitValue.map(new WorldCount(_, 1)).keyBy(_.word).sum("count")

    //scala 样例类
    val result3 = splitValue.map(x => WC(x, 1)).keyBy("word").sum("count")
    val result4 = splitValue.map(x => WC(x, 1)).keyBy(_.word).sum("count")


    env.execute(this.getClass.getSimpleName)

  }
}



