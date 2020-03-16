package com.wangyichao.bigdata.flink04

import java.util.Properties

import com.wangyichao.bigdata.flink04.source.MySQLSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * Flink 流join操作
 * 将kafka中的流和MySQL中的数据流 join
 *
 * 场景：补齐日志数据内容
 * 将日志数据中的user_id 和 MySQL中的用户信息匹配
 *
 *
 * kafka日志格式,逗号分割文本数据，user_id,domain,traffic
 *
 * 1,baidu.com,1000
 * 2.baidu.com,2000
 * 3,baidu,3000
 * 1,jd.com,1000
 * 2,jd.com,2000
 * 1,taobao.com,1000
 *
 * MySQL数据：id name age
 */

//TODO 有bug，结果不对!还要调！
object StreamingJobApp03 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inpuStream = env.addSource(this.getKafkaConsumer)

    val kafkaStream = inpuStream
      .filter(_.nonEmpty)
      .filter(_.split(",").length == 3)
      .map(x => {
        val splits = x.split(",")

        (splits(0).toInt, splits(1), splits(2).toLong)
      })

    val mysqlStream = env.addSource(new MySQLSource).broadcast

    kafkaStream.print("kafka")
    mysqlStream.print("mysql")


    kafkaStream.connect(mysqlStream).flatMap(new CoFlatMapFunction[(Int, String, Long), (Int, String, Int), (Int, String, Long, String, Int)] {
      var userInfo: (Int, String, Int) = _

      override def flatMap1(value: (Int, String, Long), out: Collector[(Int, String, Long, String, Int)]): Unit = {
        val userId = value._1
        val domain = value._2
        val traffic = value._3

//        val userId = userInfo._1
        val userName = userInfo._2
        val userAge = userInfo._3
        out.collect(userId, domain, traffic, userName, userAge)
      }

      override def flatMap2(value: (Int, String, Int), out: Collector[(Int, String, Long, String, Int)]): Unit = {
        userInfo = value
      }
    }).print()

    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 获取kafka sink
   *
   * @return
   */
  def getKafkaConsumer: FlinkKafkaConsumer[String] = {

    val topic = "ACCESS_LOG"

    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    val kafkaConsumer = new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties)
    kafkaConsumer.setStartFromEarliest()


    kafkaConsumer
  }

}
