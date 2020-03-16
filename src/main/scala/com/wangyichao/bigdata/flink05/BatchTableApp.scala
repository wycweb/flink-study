package com.wangyichao.bigdata.flink05

import com.wangyichao.bigdata.flink05.bean.Access
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object BatchTableApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //获取Stream Table的上下文
    val tableEnvironment = BatchTableEnvironment.create(env)

    //读取数据源，转换成一个DataStream
    val inputSet = env.readTextFile("data/access.data")
      .map(x => {
        val splits = x.split(",")

        Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
      })

    //注册成一张表
    val table = tableEnvironment.fromDataSet(inputSet)

    val resultTable = table
      .select("domain")
      .filter("domain!='baidu.com'")

    tableEnvironment.toDataSet[Row](resultTable).print()

  }
}
