package joiningPro;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 继承可并行的sourceFunction,并制定数据的输出类型
 */
public class StreamDataSourceA extends RichParallelSourceFunction<Tuple3<String, String, Long>> {

    /**
     * volatile: 确保本条指令不会因编译器的优化而省略。
     * 保证了一个线程修改了某个变量的值，
     * 这新值对其他线程来说是立即可见的。（实现可见性）
     */
    private volatile boolean flag = true;

    //执行程序的
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        //准备好数据
        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", "1", 1000000050000L), //[50000 - 60000）
                Tuple3.of("a", "2", 1000000054000L), //[50000 - 60000)
                Tuple3.of("a", "3", 1000000079900L), //[70000 - 80000)
                Tuple3.of("a", "4", 1000000115000L), //[110000 - 120000)  // 115000 - 5001 = 109998‬ <= 109999
                Tuple3.of("b", "5", 1000000100000L), //[100000 - 110000)
                Tuple3.of("b", "6", 1000000108000L)  //[100000 - 110000)
        };

        // 将tp3数组中的每一个tp都进行输出
        int count = 0;
        while (flag && count < elements.length) {

            ctx.collect(Tuple3.of((String) elements[count].f0,
                    (String) elements[count].f1, (Long) elements[count].f2));
            count++;

            //程序睡眠1s，保证数据已经全部发出
            Thread.sleep(1000);
        }
        //Thread.sleep(1000);
    }

    //While循环一直进行run读取处理，改变flag退出循环
    @Override
    public void cancel() {
        flag = false;
    }
}

