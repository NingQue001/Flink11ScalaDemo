package joiningPro;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkTumblingWindowsLeftJoinDemo {
    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 5002L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 一、获取两个输入流
        SingleOutputStreamOperator<Tuple3<String, String, Long>> sourceA =
                env.addSource(new StreamDataSourceA()).name("SourceA");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> sourceB =
                env.addSource(new StreamDataSourceB()).name("SourceB");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream =
                sourceA.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String,
                        Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStream =
                sourceB.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String,
                        Long>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        leftStream.coGroup(rightStream)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

        env.execute("FlinkTumblingWindowsLeftJoinDemo");
        /**
         *	最终的输出结果：
         * 	(a,1,hangzhou,1000000050000,1000000059000)
         * 	(a,2,hangzhou,1000000054000,1000000059000)
         *	(a,3,null,1000000079900,-1)
         * 	(a,4,null,1000000115000,-1)
         * 	(b,5,beijing,1000000100000,1000000105000)
         * 	(b,6,beijing,1000000108000,1000000105000)
         */
    }

    public static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String,
            Long>, Tuple5<String,
            String, String, Long, Long>> {

        // 将key相同，并且在同一窗口的数据取出来
        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> first, Iterable<Tuple3<String, String, Long>> second,
                            Collector<Tuple5<String, String, String, Long, Long>> out) throws Exception {

            for (Tuple3<String, String, Long> leftElem : first) {
                boolean hadElements = false;

                //如果左边的流join上了右边的流rightElements就不为空，就会走下面的增强for循环
                for (Tuple3<String, String, Long> rightElem : second) {
                    //将join上的数据输出
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, rightElem.f1, leftElem.f2,
                            rightElem.f2));
                    hadElements = true;
                }

                if (!hadElements) {
                    //没join上，给右边的数据赋空值
                    out.collect(new Tuple5<>(leftElem.f0, leftElem.f1, "null", leftElem.f2, -1L));
                }
            }
        }

    }

}
