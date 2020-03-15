package com.wangyichao.bigdata.flink02.source

import java.sql.{Connection, PreparedStatement}

import com.wangyichao.bigdata.flink02.bean.User
import com.wangyichao.bigdata.utils.MysqlJDBCUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MySQLSource extends RichParallelSourceFunction[User] {

  var connection: Connection = _
  var pstmt: PreparedStatement = _

  //open方法中建立连接或拿到线程池
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    connection = MysqlJDBCUtil.getConnection
    pstmt = connection.prepareStatement("SELECT * FROM user")
  }


  override def run(ctx: SourceFunction.SourceContext[User]): Unit = {
    val rs = pstmt.executeQuery()

    while (rs.next()) {
      val user = User(rs.getInt("id"), rs.getString("name"), rs.getInt("age"))

      ctx.collect(user)
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
