package com.wangyichao.bigdata.flink05

import com.wangyichao.bigdata.flink05.bean.Access
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.types.Row

object BatchSQLApp {
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
    tableEnvironment.createTemporaryView("batch_access", table)

    //查询表
    val resultTable: Table = tableEnvironment.sqlQuery("SELECT domain,sum(traffic) FROM batch_access 'baidu.com' GROUP BY domain")

    //将Table转换成DataStream
    //注意一定要标注类型，官方文档说明：convert the Table into an append DataStream of Row
    tableEnvironment.toDataSet[Row](resultTable).print()


    val result = tableEnvironment
      .from("batch_access")
      .filter("domain != 'baidu.com'")
      .groupBy("domain")
      .aggregate("sum(traffic) as traffics")
      .select("domain,traffics")

    tableEnvironment.toDataSet[Row](result).print()
  }
}
