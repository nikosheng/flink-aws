package flink.aws;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.StringUtils;

public class LocalFileWindowWordCount {
    public static void main(String[] args) {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // 待处理的文件由 --input 参数指定
        String input = params.get("input");
        if (StringUtils.isNullOrWhitespaceOnly(input)) {
            System.err.println("No port specified. Please run 'LocalFileWindowWordCount --input <input>'");
            return;
        }

        Path path = new Path(input);
        TextInputFormat textInputFormat = new TextInputFormat(path);

        // get input data by connecting to the socket
        DataStream<String> text = env.readFile(textInputFormat, input, FileProcessingMode.PROCESS_CONTINUOUSLY,2000);

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
                .flatMap((FlatMapFunction<String, WordWithCount>) (value, out) -> {
                    for (String word : value.split("\\s")) {
                        out.collect(new WordWithCount(word, 1L));
                    }
                })
                .keyBy(value -> value.word)
                .timeWindow(Time.seconds(1), Time.seconds(1))
                .reduce((ReduceFunction<WordWithCount>) (a, b) -> new WordWithCount(a.word, a.count + b.count));

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(2);
        try {
            env.execute("Local File Window WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Data type for words with count
    public static class WordWithCount {

        private String word;
        private long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
