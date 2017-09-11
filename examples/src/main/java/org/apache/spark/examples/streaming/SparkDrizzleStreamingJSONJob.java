package org.apache.spark.examples.streaming;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.Accumulator;
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
public class SparkDrizzleStreamingJSONJob implements  java.io.Serializable, Runnable {

    Accumulator<Integer> messCounter;
    transient SparkConf sparkConf;
    transient JavaStreamingContext jssc;
    String zooKeeper;
    String group;
    Map<String, Integer> topicMap;
    long startTime;

    /**
     * empty constructor
     */
    public SparkDrizzleStreamingJSONJob() {
    }


    /**
     * main method implementation
     */
    @Override
    public void run() {

        sparkConf = new SparkConf().setAppName("StreamingJob").setMaster("local[16]");

        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaPairReceiverInputDStream<String,String> messages = KafkaUtils.createStream(jssc, zooKeeper, group, topicMap);

        messCounter = jssc.sparkContext().accumulator(0, "messCount");


        try {

            messages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
                @Override
                public void call(JavaPairRDD<String, String> rdd) throws Exception {
                    rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
                        @Override
                        public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//							FileWriter fw = new FileWriter("C:\\TEMP\\output"+rdd.id()+".txt", true);
//							BufferedWriter bufferedWriter = new BufferedWriter(fw);
                            //increment accumulator value for every message call
                            messCounter.add(1);
                            JSONObject json = new JSONObject(stringStringTuple2._2);
                            System.out.println("Time for streaming (ms)"+json.getInt("message_no")+ ": "+(System.currentTimeMillis() - json.getLong("Time")));
//                            bufferedWriter.write(""+json.getInt("message_no")+","+System.currentTimeMillis()+","+(System.currentTimeMillis() - json.getLong("Time")));
//                            bufferedWriter.newLine();
//                            bufferedWriter.close();
                        }
                    });
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * return the value of accumulator called in the driver program
     */
    public Integer getAccumulator() {
        return messCounter.value();
    }
}
