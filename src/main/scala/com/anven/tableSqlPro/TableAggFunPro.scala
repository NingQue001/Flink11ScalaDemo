package com.anven.tableSqlPro

import com.anven.tableSqlPro.AggFunPro.UAggFun
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
 * UDF 之 Table Aggregate Function
 */
object TableAggFunPro {
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

    var tableAggFun = new UTableAggFun()

    // 1 Table Api
    var resultTable = sensorTalbe
      .groupBy('id)
        .flatAggregate(tableAggFun('temperature) as ('temp, 'rank))
        .select('id, 'temp, 'rank)
    resultTable.toRetractStream[Row].print("resultTable") // 此处为update操作，这里不能使用toAppendStream，需使用回撤流

    env.execute("TableAggFunPro")
  }

  class Top2TempAcc {
    var highestTemp: Double = Double.MinValue
    var secondHighestTemp: Double = Double.MinValue
  }

  /**
   * 自定义Table聚合函数,计算Top2温度值
   * 输出 (temp, rank)
   */
  class UTableAggFun extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
    override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

    def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
      if(temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if(temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    /**
     * 输出
     * @param acc
     * @param out
     */
    def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }
  }
}
