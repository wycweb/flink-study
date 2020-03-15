package com.wangyichao.bigdata.flink01.functions

import com.wangyichao.bigdata.flink01.bean.Access
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration

/**
 * 各种转换算子也会有RichXxxFunction
 * RichXxxFunction是 继承AbstractRichFunction + XxxFunction的接口实现
 *
 * AbstractRichFunction里有生命周期函数：
 * (1)open               初始化
 * (2)close              资源释放
 * (3)getRuntimeContext  拿到作业上下文
 *
 */
class AccessRichMap extends RichMapFunction[String, Access] {

  override def map(value: String): Access = {
    val splits = value.split(",")

    Access(splits(0).toLong, splits(1), splits(2).toLong)
  }

  override def open(parameters: Configuration): Unit = {
    println("~~~~执行open~~~~")

    super.open(parameters)
  }

  override def close(): Unit = {
    println("~~~~执行close~~~~")

    super.close()
  }

  override def getRuntimeContext: RuntimeContext = {
    println("~~~~执行getRuntimeContext~~~~")

    super.getRuntimeContext
  }
}
