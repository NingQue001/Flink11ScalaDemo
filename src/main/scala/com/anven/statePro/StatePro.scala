package com.anven.statePro

import com.anven.tableSqlPro.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * nc -lk 4444
 */
object StatePro {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream = env.socketTextStream("localhost", 4444)

    val sensorStream = inputStream
      .map(data => {
        var arr = data.split(",")
        SensorReading(arr(0), arr(1).toDouble, arr(2).toLong)
      })
//    sensorStream.print()

    // 需求：当温度值跳变超过10度，则报警
    val alertStream = sensorStream
      .keyBy(_.id)
      .flatMap(new TempAlertMap(10.0))
    alertStream.print("alertStream")
    env.execute("StateTest")
  }
}

class TempAlertMap(threhold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  // 懒加载，定义保存上次温度的 ValueState
  lazy val lastTempState = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double], Double.MinValue)
  )
  override def flatMap(data: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value
    if(lastTemp != Double.MinValue) {
      val diff = (data.temperature - lastTemp).abs
      if(diff >= threhold) {
        out.collect((data.id, lastTemp, data.temperature))
      }
    }

    lastTempState.update(data.temperature)
  }
}


