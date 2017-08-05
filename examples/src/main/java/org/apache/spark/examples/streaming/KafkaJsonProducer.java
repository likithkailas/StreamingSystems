package org.apache.spark.examples.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Properties;


/**
 * Created by Pai on 09-04-2017.
 */
public class KafkaJsonProducer implements Runnable {

    public KafkaJsonProducer() {
    }
    public static void main(String[] args) throws Exception{

        String topicName = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        try {

            for(int i=0; i<Integer.MAX_VALUE; i++){
                JSONObject record = new JSONObject();
                record.put("message_no", i);
                record.put("Time", System.currentTimeMillis());
                producer.send(new ProducerRecord<String, String>(topicName, ""+i, record.toString()));
            }

        }catch (JSONException e){
            e.printStackTrace();
        }
        System.out.println("Message sent successfully");
        producer.close();
    }

    @Override
    public void run() {
        try {
            KafkaJsonProducer.main(new String[] {});
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
