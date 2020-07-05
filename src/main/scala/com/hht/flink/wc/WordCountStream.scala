package com.hht.flink.wc

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    //如果流过来时候输入空格，可能会有空字符   filter(_.nonEmpty)过滤掉空字符
    import org.apache.flink.api.scala._

    val kvDStream: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1))

    val keyedStream: KeyedStream[(String, Int), Tuple] = kvDStream.keyBy(0)

    // val word2CountDataStream: DataStream[(String, Int)] = keyedStream.sum(1)

    val word2CountDataStream =  keyedStream.reduce(   (ch1, ch2) => (ch1._1, ch1._2 + ch2._2)   )

    word2CountDataStream.print()

    env.execute()
  }
}
