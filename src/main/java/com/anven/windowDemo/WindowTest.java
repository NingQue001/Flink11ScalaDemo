package com.anven.windowDemo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.DateTimeZone;
import org.apache.flink.util.Collector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author mac
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Order> orders = env.fromCollection(Arrays.asList(
                new Order(1L, "pen", 3, getTime("2020-12-30 20:01:00")),
                new Order(1L, "rubber", 2, getTime("2020-12-30 20:02:00")),
                new Order(1L, "ball", 4, getTime("2020-12-30 20:05:00")),
                new Order(1L, "bike", 1, getTime("2020-12-30 20:06:00")),

                new Order(2L, "pen", 5, getTime("2020-12-30 20:01:00")),
                new Order(2L, "rubber", 3, getTime("2020-12-30 20:02:00")),
                new Order(2L, "ball", 4, getTime("2020-12-30 20:05:00")),
                new Order(2L, "bike", 1, getTime("2020-12-30 20:06:00"))
        ));

        /**
         * 知识点：
         * 1）watermark
         * 有序数据：AscendingTimestampExtractor
         * 乱序数据：BoundedOutOfOrdernessTimestampExtractor
         */
       DataStream<String> result1 = orders
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(Order element) {
                        return element.ts;
                    }
                })
                .keyBy((KeySelector<Order, Long>) Order :: getUser)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .trigger(EventTimeTrigger.create())
                // AggregateFunction和ProcessWIndowFunction配合使用
                .aggregate(new MyAggregation(), new MyProcessWindowFunction());

       result1.print();

       env.execute("WindowTest");

    }

    public static long getTime(String strDate) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        Date date = sdf.parse(strDate);
        return date.getTime();
    }

    private static class MyAggregation implements AggregateFunction<Order, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Order order, Integer integer) {
            return integer + order.amount;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer acc1, Integer acc2) {
            return acc1 + acc2;
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Integer, String, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
            int sum = elements.iterator().next();

            String windowStart = new DateTime(context.window().getStart(), DateTimeZone.forID("+08:00")).toString("yyyy-mm-dd HH:mm:ss");
            String windowEnd = new DateTime(context.window().getEnd(), DateTimeZone.forID("+08:00")).toString("yyyy-mm-dd HH:mm:ss");

            String record = "Key: " + key + " 窗口开始时间: " + windowStart + " 窗口结束时间: " + windowEnd+" 下单商品的数量: " + sum;
            out.collect(record);
        }
    }
}
