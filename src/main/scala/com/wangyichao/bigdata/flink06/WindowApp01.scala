package com.wangyichao.bigdata.flink06

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 增量聚合的方式,底层是来一个算一个
 *
 * 在下面的例子中，每来一条数据就会处理一次，就会执行到reduce函数
 * 直到五秒的时候，会执行print方法
 */
object WindowApp01 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 7777)

    text.map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5)) //设置窗口
      .reduce((x, y) => {
        println(s"执行reduce操作：x的值为${x},y的值为${y}")
        (x._1, x._2 + y._2)
      }).print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }
}
