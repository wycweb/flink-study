package com.wangyichao.bigdata.flink02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * Flink对接kafka
 */
object StreamingJobApp04 {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaStream = env.addSource(this.getKafkaConsumer)

    kafkaStream.print()


    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 获取kafka sink
   *
   * @return
   */
  def getKafkaConsumer: FlinkKafkaConsumer[String] = {

    val topic = "METRICBEAT_TEST"

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    val kafkaConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromEarliest()


    kafkaConsumer
  }
}
