package com.wangyichao.bigdata.flink04

import com.wangyichao.bigdata.flink04.bean.Access
import com.wangyichao.bigdata.flink04.split.TrafficProcess
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, OutputTag, StreamExecutionEnvironment}

/**
 * Flink中的分流和合流
 * 分流三种方式：https://www.jianshu.com/p/274d0b78d378
 * 合流两种方式：
 * (1)Union合流时，可以合并多个数据流，但是两个DataStream的结构必修是一致的结构
 * (2)Connect合流时，连接两个数据流，数据DS的结构可以不同，Connect生成的结构是ConnectedStreams[IN,OUT]，使用时需要按需转换成正常的DataStream结构
 */
object StreamingJobApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/access.data").map(x => {
      val splits = x.split(",")
      Access(splits(0).toLong, splits(1), splits(2).toLong)
    })


    //----------------------------------分流-----------------------------------------------------------------------------------
    val tag1 = new OutputTag[Access]("big_traffic")
    val tag2 = new OutputTag[Access]("small_traffic")

    //将traffic    大于3000的 和 小于3000的拆分成两个流
    val splitStream = stream.process(new TrafficProcess(tag1, tag2))
    val bigTraffic = splitStream.getSideOutput(tag1)
    val smallTraffic = splitStream.getSideOutput(tag2)

    //获取分流的数据
    bigTraffic.print("big_traffic")
    smallTraffic.print("small_traffic")

    //---------------------------------------------------------------------------------------------------------------------


    //union合流
    bigTraffic.union(smallTraffic).print("union合流")


    //connect合流
    val smallTraffic2 = smallTraffic.map(_.domain) //改变smallTraffic结构

    val connect: ConnectedStreams[Access, String] = bigTraffic.connect(smallTraffic2)
    connect.map(x => x, y => y).print("connect合流")


    env.execute(this.getClass.getSimpleName)
  }
}
