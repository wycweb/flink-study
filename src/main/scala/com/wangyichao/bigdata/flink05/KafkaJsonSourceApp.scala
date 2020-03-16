package com.wangyichao.bigdata.flink05

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.types.Row

object KafkaJsonSourceApp {

  //https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/table/connect.html#connect-to-external-systems
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = StreamTableEnvironment.create(env)


    tableEnvironment.connect(new Kafka()
      .version("universal")
      .topic("FLINK_KAFKA_JSON_TEST")
      .startFromEarliest()
      .property("group.id", "flink_test")
      .property("bootstrap.servers", "localhost:9092")
      .property("zookeeper.connect", "localhost:2181")
    ).withFormat(new Json()
      .failOnMissingField(false)
    ).withSchema(new Schema()
      .field("user_id", DataTypes.INT())
      .field("day", DataTypes.STRING())
      .field("traffic", DataTypes.INT())
    ).inAppendMode().registerTableSink("kafka_access")

    val table = tableEnvironment.sqlQuery("select user_id from kafka_access")

    tableEnvironment.toAppendStream[Row](table).print("aaa")

    env.execute(this.getClass.getSimpleName)
  }
}
