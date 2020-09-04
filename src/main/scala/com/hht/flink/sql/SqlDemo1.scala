package com.hht.flink.sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @author hongtao.hao
  * @date 2020/9/1
  */
object SqlDemo1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[SensorReading] = env.readTextFile("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })


    //--------------------table api--------------------
    val inputTable: Table = tableEnv.fromDataStream(inputStream)

    //where 过滤
    inputTable.select("id,temperature")
      .filter("id='sensor_1'")

    inputTable.groupBy("id")
      .select("id,max(temperature)")
      .toRetractStream[(String, Double)]
      .filter(_._1)
      .print()


    //--------------------   sql   ---------------------
    tableEnv.createTemporaryView("inputTable", inputStream)

    tableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id='sensor_1'
      """.stripMargin)
      .toAppendStream[(String, Double)]
      .print()


    tableEnv.sqlQuery(
      """
        |select id,max(temperature)
        |from inputTable
        |group by id
      """.stripMargin)
      //todo toRetractStream 和 toAppendStream的区别 ,如果了使用 groupby，table 转换为流的时候只能用 toRetractDstream

      .toRetractStream[(String, Double)]
      //todo toRetractDstream 得到的第一个 boolean 型字段标识 true 就是最新的数据
      //todo (Insert)，false 表示过期老数据(Delete)
      //    (true,(sensor_1,35.8))
      //    (true,(sensor_6,15.4))
      //    (true,(sensor_7,6.7))
      //    (true,(sensor_10,38.1))
      //    (false,(sensor_1,35.8))
      //    (true,(sensor_1,37.9))
      //    (false,(sensor_1,37.9))
      //    (true,(sensor_1,39.0))
      .filter(_._1)
      .print()
    env.execute("table api example job")
  }


}
