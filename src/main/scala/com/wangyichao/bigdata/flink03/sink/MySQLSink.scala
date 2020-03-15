package com.wangyichao.bigdata.flink03.sink

import java.sql.{Connection, PreparedStatement}

import com.wangyichao.bigdata.flink03.bean.User
import com.wangyichao.bigdata.utils.MysqlJDBCUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySQLSink extends RichSinkFunction[User] {
  var connection: Connection = _
  var insertPstmt: PreparedStatement = _
  var updatePstmt: PreparedStatement = _

  //建立连接等
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = MysqlJDBCUtil.getConnection
    insertPstmt = connection.prepareStatement("INSERT INTO user(name,age) VALUES (?, ?)")
    updatePstmt = connection.prepareStatement("UPDATE user SET name = ? ,age = ? WHERE name = ?")
  }

  //执行操作
  override def invoke(value: User, context: SinkFunction.Context[_]): Unit = {
    val name = value.name
    val age = value.age

    updatePstmt.setString(1, name)
    updatePstmt.setInt(2, age)
    updatePstmt.setString(3, name)
    updatePstmt.execute()

    if (updatePstmt.getUpdateCount == 0) {
      insertPstmt.setString(1, name)
      insertPstmt.setInt(2, age)
      insertPstmt.execute()
    }

  }

  //释放资源
  override def close(): Unit = {
    super.close()

    MysqlJDBCUtil.closeConnection(connection, insertPstmt)
    MysqlJDBCUtil.closeConnection(connection, updatePstmt)
  }

}
