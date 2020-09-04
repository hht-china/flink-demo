package com.hht.flink.sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  * @author hongtao.hao
  * @date 2020/9/2
  */
object OutputDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //todo 定义输出表
    tableEnv.connect(new FileSystem().path("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\output.txt"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("outputTable")

    val inputStream: DataStream[SensorReading] = env.readTextFile("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
    tableEnv.createTemporaryView("inputTable", inputStream)
    tableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id='sensor_1'
      """.stripMargin)
      //todo 输出，文件名重复会报错  怎么解决？？
      .insertInto("outputTable")

    env.execute("table api output job")
  }
}
