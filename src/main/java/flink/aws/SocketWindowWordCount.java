package flink.aws;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接 socket 获取输入数据，这里连接到本地9000端口
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // 解析数据，按 word 分组，开窗，聚合
        DataStream<Tuple2<String, Integer>> wordCounts;
        wordCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word : value.split("\\s")) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                });


        DataStream<Tuple2<String, Integer>> windowCounts = wordCounts
                .keyBy(0)
                .timeWindowAll(Time.seconds(5))
                .sum(1);

        windowCounts.print().setParallelism(1);
        try {
            env.execute("Socket Window WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
