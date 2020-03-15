package com.wangyichao.bigdata.flink04.partition

import org.apache.flink.api.common.functions.Partitioner

class AccessPartition extends Partitioner[String] {

  override def partition(key: String, numPartitions: Int): Int = {

    if (key == "baidu.com") {
      0
    } else if (key == "taobao.com") {
      1
    } else {
      2
    }
  }
}
