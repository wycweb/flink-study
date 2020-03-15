package com.wangyichao.bigdata.flink01.functions

import com.wangyichao.bigdata.flink01.bean.Access
import org.apache.flink.api.common.functions.FilterFunction

//各种转换算子都有 XxxFunction
class AccessFilter(traffic: Long) extends FilterFunction[Access] {

  override def filter(value: Access): Boolean = value.traffic > traffic
}
