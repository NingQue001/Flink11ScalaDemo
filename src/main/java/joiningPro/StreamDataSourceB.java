package joiningPro;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class StreamDataSourceB extends RichParallelSourceFunction<Tuple3<String, String, Long>> {

    private volatile boolean flag = true;

    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {

        // a ,1 hangzhou
        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "hangzhou", 1000000059000L), //[50000, 60000)
                Tuple3.of("b", "beijing",  1000000105000L), //[100000, 110000)
        };

        int count = 0;
        while (flag && count < elements.length) {
            //将数据发出去
            ctx.collect(new Tuple3<>((String) elements[count].f0,
                    (String) elements[count].f1, (long) elements[count].f2));

            count++;
            Thread.sleep(1000);
        }

        //Thread.sleep(100000000);
    }

    @Override
    public void cancel() {
        flag = false;
    }
}


