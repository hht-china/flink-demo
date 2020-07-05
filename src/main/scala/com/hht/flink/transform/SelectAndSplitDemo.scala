package com.hht.flink.transform

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object SelectAndSplitDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dStream: DataStream[String] = env.socketTextStream("localhost", 9999)

  }
}