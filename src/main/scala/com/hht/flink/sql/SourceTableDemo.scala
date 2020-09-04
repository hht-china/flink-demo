package com.hht.flink.sql

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
/**
  * @author hongtao.hao
  * @date 2020/9/2
  */
object SourceTableDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt"

    //读取文件可能需要切分字段等，直接读取表比较方便
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) // 定义从外部文件读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temperature", DataTypes.DOUBLE())
    ) // 定义表的结构
      .createTemporaryTable("inputTable") // 在表环境注册一张表

    // todo sql 转Table api
    val sensorTable: Table = tableEnv.from("inputTable")

    tableEnv.sqlQuery(
      """
        |select id
        |from inputTable
      """.stripMargin)
      .toAppendStream[(String)]
      .print()

  }
}
