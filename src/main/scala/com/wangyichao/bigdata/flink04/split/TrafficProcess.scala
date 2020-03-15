package com.wangyichao.bigdata.flink04.split

import com.wangyichao.bigdata.flink04.bean.Access
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

class TrafficProcess(tag1: OutputTag[Access], tag2: OutputTag[Access]) extends ProcessFunction[Access, Access] {

  override def processElement(access: Access, ctx: ProcessFunction[Access, Access]#Context, out: Collector[Access]): Unit = {

    val traffic = access.traffic

    if (traffic > 3000) {
      ctx.output(tag1, access)
    } else {
      ctx.output(tag2, access)
    }

  }
}
