package com.hht.flink.sql.udftest

import com.hht.flink.sql.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/*
    lateral table(split(id)) as splitid(word, length)
    一对多，一行输出多行

result> sensor_1,2019-01-17 09:43:19.0,word_splitsensor,word_length=6
result> sensor_1,2019-01-17 09:43:19.0,word_split1,word_length=1
sql> sensor_1,2019-01-17 09:43:19.0,word_splitsensor,word_length=6
sql> sensor_1,2019-01-17 09:43:19.0,word_split1,word_length=1
result> sensor_6,2019-01-17 09:43:21.0,word_splitsensor,word_length=6
result> sensor_6,2019-01-17 09:43:21.0,word_split6,word_length=1
sql> sensor_6,2019-01-17 09:43:21.0,word_splitsensor,word_length=6
sql> sensor_6,2019-01-17 09:43:21.0,word_split6,word_length=1
result> sensor_7,2019-01-17 09:43:22.0,word_splitsensor,word_length=6
result> sensor_7,2019-01-17 09:43:22.0,word_split7,word_length=1
sql> sensor_7,2019-01-17 09:43:22.0,word_splitsensor,word_length=6
sql> sensor_7,2019-01-17 09:43:22.0,word_split7,word_length=1
result> sensor_10,2019-01-17 09:43:25.0,word_splitsensor,word_length=6
result> sensor_10,2019-01-17 09:43:25.0,word_split10,word_length=2
sql> sensor_10,2019-01-17 09:43:25.0,word_splitsensor,word_length=6
sql> sensor_10,2019-01-17 09:43:25.0,word_split10,word_length=2
result> sensor_1,2019-01-17 09:43:27.0,word_splitsensor,word_length=6
result> sensor_1,2019-01-17 09:43:27.0,word_split1,word_length=1
sql> sensor_1,2019-01-17 09:43:27.0,word_splitsensor,word_length=6
sql> sensor_1,2019-01-17 09:43:27.0,word_split1,word_length=1
result> sensor_1,2019-01-17 09:43:28.0,word_splitsensor,word_length=6
result> sensor_1,2019-01-17 09:43:28.0,word_split1,word_length=1
sql> sensor_1,2019-01-17 09:43:28.0,word_splitsensor,word_length=6
sql> sensor_1,2019-01-17 09:43:28.0,word_split1,word_length=1
result> sensor_1,2019-01-17 09:43:29.0,word_splitsensor,word_length=6
result> sensor_1,2019-01-17 09:43:29.0,word_split1,word_length=1
sql> sensor_1,2019-01-17 09:43:29.0,word_splitsensor,word_length=6
sql> sensor_1,2019-01-17 09:43:29.0,word_split1,word_length=1
 */
object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("C:\\Users\\ITO-user\\Desktop\\flink-demo\\src\\main\\resources\\sensor.txt")
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )

    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 先创建一个UDF对象
    val split = new Split("_")

    // Table API调用
    val resultTable = sensorTable
      .joinLateral( split('id) as ('word, 'length) )    // 侧向连接，应用TableFunction
      .select('id, 'ts, 'word, 'length)

    // SQL 调用
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, ts, word, length
        |from
        |sensor, lateral table(split(id)) as splitid(word, length)
      """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("table function test job")
  }

  // 自定义TableFunction，实现分割字符串并统计长度(word, length)
  class Split(separator: String) extends TableFunction[(String, String)]{
    def eval( str: String ): Unit ={
      str.split(separator).foreach(
        word => collect(("word_split"+word, "word_length="+word.length))
      )
    }
  }
}


