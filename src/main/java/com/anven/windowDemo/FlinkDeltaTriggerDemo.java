package com.anven.windowDemo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.DateTimeZone;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

/**
 * GlobalWindow + DeltaTrigger实现按位置触发窗口
 *
 * 常用触发器：
 * 1）DeltaTrigger：当前元素与触发上一个窗口的元素相比较，如果达到阈值，则触发
 * 2）EventTimeTrigger：􏰚􏰟􏰠􏰡􏰆􏰚􏰟􏰠􏰡􏰆一次触发，watermark大于窗口结束时触发􏰢􏰣􏰤􏰥􏰦􏰧􏰨􏰩􏰨􏰠􏰡􏰢􏰣􏰤􏰥􏰦􏰧􏰨􏰩􏰨􏰠􏰡
 * 3）ProcessingTimeTrigger：一次触发，machine time大于窗口结束时触发􏰢􏰣􏰤􏰥􏰦􏰧􏰨􏰩􏰨􏰠􏰡􏰢􏰣􏰤􏰥􏰦􏰧􏰨􏰩􏰨􏰠􏰡
 * 4）ContinuousEventTimeTrigger：多次触发，基于EventTime的固定时间间隔
 * 5）ContinuousProcessingTimeTrigger：多次触发，基于ProcessingTime的固定时间间隔
 * 6）CountTrigger：多次触发，基于element的固定条数
 * 7）PurgingTrigger：trigger wrapper，当nested trigger触发时，额外清除窗口内的数据
 *
 */
public class FlinkDeltaTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间属性
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 数据结构：id，速度，位移，时间戳
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData = env.fromCollection(Arrays.asList(
                new Tuple4(1, 90, 0d, getTime("2020-12-30 20:01:00")),
                new Tuple4(1, 85, 3000d, getTime("2020-12-30 20:02:00")),
                new Tuple4(1, 200, 6000d, getTime("2020-12-30 20:03:00")),
                new Tuple4(1, 100, 10002d, getTime("2020-12-30 20:04:00")),

                new Tuple4(1, 90, 10006d, getTime("2020-12-30 20:05:00")),
                new Tuple4(1, 299, 15000d, getTime("2020-12-30 20:07:00")),
                new Tuple4(1, 175, 20019d, getTime("2020-12-30 20:06:00")),

                new Tuple4(2, 90, 0d, getTime("2020-12-30 20:08:00")),
                new Tuple4(2, 85, 3000d, getTime("2020-12-30 20:10:00")),
                new Tuple4(2, 185, 10005d, getTime("2020-12-30 20:09:00")),

                new Tuple4(2, 150, 10006d, getTime("2020-12-30 20:12:00")),
                new Tuple4(2, 184, 21000d, getTime("2020-12-30 20:11:00"))
        ));

        DataStream<String> topSpeeds = carData
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Integer, Integer, Double, Long>>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
                        return element.f3;
                    }
                })
                .keyBy(0)
                // 全局窗口
                .window(GlobalWindows.create())
                // DeltaTrigger，计算delta值，达到阈值则触发
                .trigger(DeltaTrigger.of(10000,
                        new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                            @Override
                            public double getDelta(Tuple4<Integer, Integer, Double, Long> oldDataPoint, Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                                return newDataPoint.f2 - oldDataPoint.f2;
                            }
                        }, carData.getType().createSerializer(env.getConfig())))
                // 驱逐器
                .evictor(new Evictor<Tuple4<Integer, Integer, Double, Long>, GlobalWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple4<Integer, Integer, Double, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

                    }

                    /*TODO 窗口触发后当前窗口数据，便于统计每个窗口的最大速度，而不至于收到前面窗口数据的影响*/
                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple4<Integer, Integer, Double, Long>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
                        Iterator<TimestampedValue<Tuple4<Integer, Integer, Double, Long>>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            iterator.next();
                            iterator.remove();
                        }

                    }
                })
                // 聚合
                .aggregate(new MyAggregateFunction(), new ProcessWindowFunction<Integer, String, Tuple, GlobalWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        out.collect("Key" + tuple.toString() + " TopSpeed: " + elements.iterator().next());
                    }
                });

        topSpeeds.print();

        env.execute();

    }

    public static long getTime(String strDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        Date date = sdf.parse(strDate);
        return date.getTime();
    }

    private static class MyAggregateFunction implements AggregateFunction<Tuple4<Integer, Integer, Double, Long>, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        /*TODO 取最大值*/
        @Override
        public Integer add(Tuple4<Integer, Integer, Double, Long> value, Integer accumulator) {
            return value.f1 > accumulator ? value.f1 : accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        /*TODO 取最大值*/
        @Override
        public Integer merge(Integer a, Integer b) {
            return a > b ? a : b;
        }
    }
}
