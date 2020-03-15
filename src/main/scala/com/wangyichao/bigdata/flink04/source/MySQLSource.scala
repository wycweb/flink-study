package com.wangyichao.bigdata.flink04.source

import java.sql.{Connection, PreparedStatement}

import com.wangyichao.bigdata.flink04.bean.User
import com.wangyichao.bigdata.utils.MysqlJDBCUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MySQLSource extends RichParallelSourceFunction[(Int, String, Int)] {

  var connection: Connection = _
  var pstmt: PreparedStatement = _

  //open方法中建立连接或拿到线程池
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    connection = MysqlJDBCUtil.getConnection
    pstmt = connection.prepareStatement("SELECT * FROM user")
  }


  override def run(ctx: SourceFunction.SourceContext[(Int, String, Int)]): Unit = {
    val rs = pstmt.executeQuery()

    while (rs.next()) {

      ctx.collect((rs.getInt("id"), rs.getString("name"), rs.getInt("age")));
    }
  }

  override def cancel(): Unit = {

  }

  //关闭连接
  override def close(): Unit = {
    super.close()
    MysqlJDBCUtil.closeConnection(connection, pstmt)
  }

}
