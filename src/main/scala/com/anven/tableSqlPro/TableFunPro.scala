package com.anven.tableSqlPro

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * UDF
 * 1 ScalarFunction
 * 2 TableFunction
 */
object TableFunPro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 读取数据
    val inputStream: DataStream[String] = env.readTextFile("file:///Users/mac/IdeaProjects/Flink11ScalaDemo/src/main/resources/sensor.txt")

    // 得到样例类 SensorReading 的一个 DataStream
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toDouble, dataArray(2).toLong)
      /**分配时间戳和水印  */
    }).assignTimestampsAndWatermarks(assigner = new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.ts
    })

    // 将流转化为表
    // 使用Group Window时，必须指定rowtime！！！
    val sensorTalbe: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'ts.rowtime())

    val split = new Split("_")

    // 1 Table API
    val resutlTable = sensorTalbe
      .joinLateral( split('id) as ('word, 'length) )
      .select('id, 'temperature, 'ts, 'word, 'length)
//    resutlTable.toAppendStream[Row].print("resutlTable")

    // 2 SQL API
    tableEnv.createTemporaryView("sensor", sensorTalbe)
    tableEnv.registerFunction("split", split)
    val sqlResultTable = tableEnv.sqlQuery(
      """
        |SELECT id, temperature, ts, word, length
        |FROM sensor, LATERAL TABLE( split(id) ) AS splitId(word, length)
        |""".stripMargin)
    sqlResultTable.toAppendStream[Row].print("sqlResultTable")

    env.execute("TableFunPro")
  }

  /**
   * 自定义表函数
   */
  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(value: String): Unit = {

//      value.split(separator).foreach(
//        word => collect((word, word.length))
//      )

      val strings = value.split(separator)
      collect(strings(0), strings(1).toInt)
    }
  }
}
