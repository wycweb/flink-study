package com.wangyichao.bigdata.flink05

import com.wangyichao.bigdata.flink05.bean.Access
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object  StreamSQLApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //获取Stream Table的上下文
    val tableEnvironment = StreamTableEnvironment.create(env)

    //读取数据源，转换成一个DataStream
    val inputStream = env.readTextFile("data/access.data")

    val mapStream = inputStream.map(x => {
      val splits = x.split(",")

      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    })

    //注册成一张表
    val table = tableEnvironment.fromDataStream(mapStream)
    tableEnvironment.createTemporaryView("access", table)

    //查询表
    val resultTable: Table = tableEnvironment.sqlQuery("SELECT * FROM access")

    //结合Dynamic Tables中的概念，这个SQL语句并不是简单的对应Dynamic Tables 中的insert into的模型，所以，只要涉及到更新、删除操作类型的SQL，最后执行的时候用Dynamic Tables就会报错
    val resultTable2: Table = tableEnvironment.sqlQuery("SELECT domain,sum(traffic) FROM access GROUP BY domain")

    //将Table转换成DataStream
    //注意一定要标注类型，官方文档说明：convert the Table into an append DataStream of Row
    tableEnvironment.toAppendStream[Row](resultTable).print("aaa")
    tableEnvironment.toRetractStream[Row](resultTable2).print("bbb")


    env.execute(this.getClass.getSimpleName)
  }


}
