package com.anven.tableSqlPro

import com.anven.funPro.UTCToLocal
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * Table API
 * Window 聚合例子
 */
object WindowAggPro {
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
      /**TODO 分配时间戳和水印  */
    }).assignTimestampsAndWatermarks(assigner = new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.ts /**TODO 乘以1000起什么作用？？？  */
    })

    // 将流转化为表
    // 使用Group Window时，必须指定rowtime！！！
    val sensorTalbe: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'ts.rowtime)
//     sensorTalbe.toAppendStream[Row].print("resultTalbe：")

    // 1 Table Api
    // 1.1 Group Window聚合操作
    val aggTable = sensorTalbe
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.start, 'tw.end)
//    aggTable.toAppendStream[Row].print("aggTable: ")

    // 1.2 Over Window聚合
    val overResultTable = sensorTalbe
      /** 疑问：2.rows时，id.count在第四个时不准确（正常应该是4，结果却是3），需研究？？？
       * 正常的，preceding（前面的），preceding 2.rows 表示当前元素和前面两个元素做一个聚合
       * */
      .window(Over partitionBy 'id orderBy 'ts preceding 3.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
//    overResultTable.toAppendStream[Row].print("overResultTable")

    // 2 SQL Api
    // 2.1 Group Windows
    tableEnv.createTemporaryView("sensor", sensorTalbe)
    // 注册函数
    tableEnv.registerFunction("utctolocal", new UTCToLocal());
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, count(id), utctolocal(hop_end(ts, interval '4' second, interval '10' second))
        |from sensor
        |group by id, hop(ts, interval '4' second, interval '10' second)
        |""".stripMargin)
//    resultSqlTable.printSchema()
//    resultSqlTable.toAppendStream[Row].print("resultSqlTable")

    // 2.2 Over Windows
    val overSqlTable = tableEnv.sqlQuery(
      """
        |select id, ts, count(id) over w, avg(temperature) over w
        |from sensor
        |window w as (
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
        |""".stripMargin)
    overSqlTable.toAppendStream[Row].print("overSqlTable")

    env.execute("TimeAndWindowTest")
  }

}
