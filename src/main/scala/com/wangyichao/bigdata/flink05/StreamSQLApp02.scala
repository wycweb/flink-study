package com.wangyichao.bigdata.flink05

import com.wangyichao.bigdata.flink05.bean.{Access, WC}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object StreamSQLApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取Stream Table的上下文
    val tableEnvironment = StreamTableEnvironment.create(env)

    //读取数据源，转换成一个DataStream
    val inputStream = env.socketTextStream("localhost", 7777)

    val mapStream = inputStream
      .flatMap(_.split(","))
      .map(WC(_, 1))

    //注册成一张表
    val table = tableEnvironment.fromDataStream(mapStream)

    //Append-only stream，不做任何处理，仅仅生成简单的Dynamic Tables
//    tableEnvironment.toAppendStream[Row](table).print()


    tableEnvironment.createTemporaryView("wc", table)
    val resultTable = tableEnvironment.sqlQuery("SELECT word,count(cnt) cnts FROM wc GROUP BY word")

    /**
     * tableEnvironment.toAppendStream[Row](resultTable)会报错 Table is not an append-only table. Use the toRetractStream()
     * 官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/streaming/dynamic_tables.html#table-to-stream-conversion
     * 生成的结果中toRetractStream生成的结果为 （boolean,Row）
     * true代表插入的值，false代表来了新值，旧值被删除
     */
    val finalData = tableEnvironment.toRetractStream[Row](resultTable)
    finalData.print()

    env.execute(this.getClass.getSimpleName)
  }


}
