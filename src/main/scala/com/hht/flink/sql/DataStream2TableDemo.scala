package com.hht.flink.sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @author hongtao.hao
  * @date 2020/9/1
  */
object DataStream2TableDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)


    val inputStream: DataStream[SensorReading] = env.readTextFile("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    //todo TableEnvironment tableApi或flinksql的运行环境
    //todo 另外flinksql一般要引入这个依赖 import org.apache.flink.table.api.scala._
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //------------------------------------------------------------
    //todo 将DataStream直接注册表
    tableEnv.createTemporaryView("inputTable", inputStream)

    //------------------------------------------------------------
    //todo 将DataStream转为Table,可以使用Table Api
    val inputTable: Table = tableEnv.fromDataStream(inputStream)

    //------------------------------------------------------------
    //todo Table也可以注册为表
    //tableEnv.createTemporaryView("inputTable",inputTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from inputTable
      """.stripMargin)

    //todo 查看执行计划
    println(tableEnv.explain(resultSqlTable))


    // todo 把表转换成流
    resultSqlTable.toAppendStream[SensorReading]
      //打印输出
      .print()

    env.execute("table api example job")
  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)
