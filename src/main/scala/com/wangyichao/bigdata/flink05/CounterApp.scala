package com.wangyichao.bigdata.flink05

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

object CounterApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("a", "b", "c", "d")

    val result = data.map(new RichMapFunction[String, String] {

      //定义计数器
      private val counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //注册累加器对象
        getRuntimeContext.addAccumulator("cnt", this.counter)
      }

      override def map(value: String): String = {
        //在operator环境中使用累加器
        this.counter.add(1)

        value
      }
    })

    result.writeAsText("output/a.log")

    val jobExecutionResult = env.execute(this.getClass.getSimpleName)

    //获取计数器的值
    val counter: Int = jobExecutionResult.getAccumulatorResult("cnt")
    println(counter)
  }
}
