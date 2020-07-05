package com.hht.flink.kafka

import com.hht.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val dStream: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("flink-topic"))

    dStream.print()

    env.execute()
  }
}
