package com.wangyichao.bigdata.flink06

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * 全量聚合：把窗口中所有的数据准备完毕，等时间到了，在遍历所有数据计算
 * 可以对窗口中的数据进行排序
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#processwindowfunction
 *
 * 求窗口内的最大值
 * 6 5 11 10
 */
object WindowApp02 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 7777)

    text.map(x => (1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5)) //设置窗口
      .process(new MyProcessWindowFunction)
      .print().setParallelism(1)


    env.execute(this.getClass.getSimpleName)
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[(Int, Int), String, Tuple, TimeWindow] {

  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[String]): Unit = {

    var maxValue = Int.MinValue

    for (ele <- elements) {
      maxValue = ele._2.max(maxValue)
    }

    out.collect(s"Window ${context.window} maxVlue:$maxValue")

  }

}
