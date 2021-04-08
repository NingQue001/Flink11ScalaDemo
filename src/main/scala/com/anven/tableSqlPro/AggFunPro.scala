package com.anven.tableSqlPro

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * UDF 之 Aggregate Function
 */
object AggFunPro {
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

      /** 分配时间戳和水印  */
    }).assignTimestampsAndWatermarks(assigner = new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.ts
    })

    // 将流转化为表
    // 使用Group Window时，必须指定rowtime！！！
    val sensorTalbe: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'ts.rowtime())

    var aggFun = new UAggFun()

    // 1 Table Api
    var resultTable = sensorTalbe
      .groupBy('id)
      .aggregate(aggFun('temperature) as 'avgTem)
      .select('id, 'avgTem)
    resultTable.toRetractStream[Row].print("resultTable") // 此处为update操作，这里不能使用toAppendStream，需使用回撤流

    env.execute("AggFunPro")
  }

  class AvgAcc {
    var sum: Double = 0.0
    var count: Int = 0
  }

  /**
   * 自定义聚合函数
   */
  class UAggFun extends AggregateFunction[Double, AvgAcc] {
    override def getValue(accumulator: AvgAcc): Double = accumulator.sum / accumulator.count

    override def createAccumulator(): AvgAcc = new AvgAcc()

    def accumulate(acc: AvgAcc, tmp: Double): Unit = {
      acc.sum += tmp
      acc.count += 1
    }
  }
}
