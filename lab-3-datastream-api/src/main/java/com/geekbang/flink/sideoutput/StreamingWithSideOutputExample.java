package com.geekbang.flink.sideoutput;

import com.geekbang.flink.wordcount.util.WordCountData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class StreamingWithSideOutputExample {

    // side output tag with explicit type info
    private static final OutputTag<String> rejectedWordsTag =
            new OutputTag<>("rejected", org.apache.flink.api.common.typeinfo.Types.STRING);

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            text = env.fromElements(WordCountData.WORDS);
        }

        // tokenize
        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .process(new Tokenizer());

        // side output stream
        DataStream<String> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return "rejected: " + value;
                    }
                });

        // main stream aggregation using processing time window
        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // emit result
        if (params.has("output")) {
            counts.print();       
            rejectedWords.print();
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
            rejectedWords.print();
        }

        env.execute("Streaming WordCount SideOutput Example");
    }

    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {

        @Override
        public void processElement(String value, Context context, Collector<Tuple2<String, Integer>> collector) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 5) {
                    context.output(rejectedWordsTag, token);
                } else if (!token.isEmpty()) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}

