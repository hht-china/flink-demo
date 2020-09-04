package com.hht.flink.sql

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{GroupWindow, Over, Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author hongtao.hao
  * @date 2020/9/2
  */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[SensorReading] = env.readTextFile("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt")
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      //todo 设置水位线
      .assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    //todo 手动指定字段名转换表
    /** todo
      * 根据指定的.rowtime 字段名是否存在于数据流的架构中，timestamp 字段可以：
      * ⚫ 作为新字段追加到 schema
      * ⚫ 替换现有字段
      */
    val inputTable: Table = tableEnv.fromDataStream(inputStream,'id,'temperature,'timestamp,'timestamp.rowtime as 'ts)
     tableEnv.createTemporaryView("inputTable",inputTable)

    //开窗要基于什么时间开,也就是上面定义的时间语义ts
    inputTable.window(Tumble over 10.minutes on 'ts as 'w)
      .groupBy('id,'w)
      .select('id,'id.count,'w.end)
      .toRetractStream[Row]
      .print("滚动窗口>")


    //------------------  over  window  --------------------
    /**
      * 无界  从最开始到当前数据
      *             UNBOUNDED_RANGE 是常亮
      *  按照行数   用UNBOUNDED_ROW
      *
      *
      * 有界
      *   需要指定时间或者行数
      *   .window(Over partitionBy 'a orderBy 'rowtime preceding 1.minutes as 'w)
      *   .window(Over partitionBy 'a orderBy 'rowtime preceding 10.rows as 'w)
      */
    //含义是当前数据和之前的两条数据的结果
    inputTable.window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'w)
      //  over 和sparksql和hive等over差不多 是不能聚合的  .groupBy('id,'w)
      // 每个统计需要用over 'w 指定over窗口
      .select('id,'id.count over 'w,'temperature.avg over 'w)
      .toRetractStream[Row]
      .print("over window >")



    //------------------------  sql  ----------------------
    tableEnv.sqlQuery(
      """
        |select id,count(*),tumble_end(ts,interval '10' second)
        |from inputTable
        |group by id,tumble(ts,interval '10' second)
      """.stripMargin)
      .toRetractStream[Row]
      .print("sql 滚动窗口>")


    tableEnv.sqlQuery(
      """
        |select id,ts,count(id) over w,avg(temperature) over w
        |from inputTable
        |window w as(
        |  partition by id
        |  order by ts
        |  rows between 2 preceding and current row
        |)
      """.stripMargin)
      .toRetractStream[Row]
      .print("sql over window >")

    env.execute("window test ")

  }
}
