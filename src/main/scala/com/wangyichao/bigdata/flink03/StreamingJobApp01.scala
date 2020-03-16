package com.wangyichao.bigdata.flink03

import com.wangyichao.bigdata.flink03.bean.User
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import com.wangyichao.bigdata.flink03.sink.MySQLSink

/**
 * 自定义MySQL sink
 */
object StreamingJobApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/user.data").map(x => {
      val splits = x.split(",")
      User(splits(0), splits(1).toInt)
    })

    stream.addSink(new MySQLSink)

    env.execute(this.getClass.getSimpleName)
  }
}
