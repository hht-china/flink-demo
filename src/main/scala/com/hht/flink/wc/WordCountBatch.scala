package com.hht.flink.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val input = "C:\\Users\\Administrator\\Desktop\\a.txt"
    val dataSet: DataSet[String] = env.readTextFile(input)

    import org.apache.flink.api.scala._
    //val kvDataSet: DataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1))

    //kvDataSet
   val aggregateDataSet: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggregateDataSet.print()
  }
}
