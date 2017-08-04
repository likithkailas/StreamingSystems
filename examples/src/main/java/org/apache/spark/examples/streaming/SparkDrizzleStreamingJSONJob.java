package org.apache.spark.examples.streaming;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by likit on 25-Jun-17.
 */
public class SparkDrizzleStreamingJSONJob {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {


        if (args.length < 4) {
            System.err.println("Usage: StreamingJob <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("StreamingJob").setMaster("local[16]");
        // Create the context with 1 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        messages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
            @Override
            public void call(JavaPairRDD<String, String> rdd) throws Exception {
                System.out.println("Messages per sec: "+rdd.count());
                rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
                    @Override
                    public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                        JSONObject json = new JSONObject(stringStringTuple2._2);
//                        System.out.println("Time for streaming (ms): " +(System.currentTimeMillis() - json.getLong("Time")));
                    }
                });
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }

}
