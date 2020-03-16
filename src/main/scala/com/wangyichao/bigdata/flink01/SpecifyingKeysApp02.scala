package com.wangyichao.bigdata.flink01

import com.wangyichao.bigdata.flink01.bean.Access
import com.wangyichao.bigdata.flink01.functions.{AccessFilter, AccessRichMap}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SpecifyingKeysApp02 {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度方便测试
    env.setParallelism(3)

    // 接收数据
    val text = env.readTextFile("data/access.data")


    //常规
    val accessStream = text.map(x => {
      val splits = x.split(",")

      Access(splits(0).toLong, splits(1), splits(2).toLong)
    })

    accessStream.filter(_.traffic > 4000).print()


    //指定转换函数：https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/api_concepts.html#specifying-transformation-functions
    text.map(new AccessRichMap).filter(new AccessFilter(4000)).print()


    env.execute(this.getClass.getSimpleName)

  }
}




