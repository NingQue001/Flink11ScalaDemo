package com.anven.tableSqlPro

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object TableExample extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val inputStream: DataStream[String] = env.readTextFile("file:///usr/local/data/flink/sensor.txt")

  // 得到样例类SensorReading的一个DataStream
  val dataStream: DataStream[SensorReading] = inputStream.map(data => {
    val dataArray = data.split(",")
    SensorReading(dataArray(0), dataArray(1).toDouble, dataArray(2).toLong)
  })

  // 表执行环境
  val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
  
  // DataStream转化为Table
  val dataTable: Table = tableEnv.fromDataStream(dataStream)

  // 调用table api
  val resultTable: Table = dataTable.select("id, temperature")
    .filter("id == '1' ")

  // 使用隐式转换为DataStream
  val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
  resultStream.print()

  // 创建视图
  tableEnv.createTemporaryView("sensor", dataStream)
  // 使用Flink SQL
  val sqlTable: Table = tableEnv.sqlQuery(
    """
    |select id, temperature
    |from sensor
    |where id = '2'
    """.stripMargin)  // 使用scala字符拼接
  sqlTable.toAppendStream[(String, Double)].print()

  env.execute("TableExample")
}
