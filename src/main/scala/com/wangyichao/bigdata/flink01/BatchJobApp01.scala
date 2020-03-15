package com.wangyichao.bigdata.flink01

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object BatchJobApp01 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("data/wc.data")

    val result = text.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    result.print()
  }

}