package org.apache.spark.examples.streaming;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static com.sun.xml.internal.ws.dump.LoggingDumpTube.Position.Before;

/**
 * Created by Pai on 09-04-2017.
 */
public class KafkaTests {

    KafkaServerStartable kafka = null;
    HBaseTestingUtility htu = null;
    String zookeeperHost = "";
    String zookeeperPort = "";
    Properties kafkaProps = null;
    String zookeeperConnect;
    SparkDrizzleStreamingJSONJob sparkJob;
    String topicName;
    Thread SparkJobThread;

    /**
     * start ZK and Kafka Broker.
     * @throws InterruptedException
     */
    @Before
    public void initialize() throws InterruptedException {

        System.setProperty("hadoop.home.dir", "c:/winutils/");

        String path = "/tmp/kafka-logs";
        final File kafkaLogs = new File(path);
        try {
            FileUtils.deleteDirectory(kafkaLogs);
        } catch (IOException e) {
            e.printStackTrace();
        }


        htu = HBaseTestingUtility.createLocalHTU();
        try {
            htu.cleanupTestDir();
            htu.startMiniZKCluster();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        Configuration hbaseConf = htu.getConfiguration();

        zookeeperHost = hbaseConf.get("hbase.zookeeper.quorum");
        zookeeperPort = hbaseConf.get("hbase.zookeeper.property.clientPort");
        zookeeperConnect = String.format("%s:%s", zookeeperHost, zookeeperPort);

        kafkaProps = new Properties();
        kafkaProps.put("broker.id", "1");
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("zookeeper.connect", zookeeperConnect);
        kafkaProps.put("client.id", "KafkaSuite");
        kafkaProps.put("group.id", "test-group");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("delete.topic.enable", "true");
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();

        sparkJob = new SparkDrizzleStreamingJSONJob();

        sparkJob.group = "my-consumer-group";
        sparkJob.zooKeeper = zookeeperConnect;

        int numThreads = 1;
        topicName = "test";

        sparkJob.topicMap = new HashMap<>();
        String[] topics = topicName.split(",");
        for (String topic : topics) {
            sparkJob.topicMap.put(topic, numThreads);
        }

        try {
            SparkJobThread =new Thread(sparkJob);
            SparkJobThread.start();
//			sparkJob.run();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Thread.sleep(10000);


    }

    /**
     * test directory mode of the trip generator.
     * @throws Exception
     */
    @Test
    public void someKafkaTest() throws Exception {

        String topicName = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        long startTime = 0;
        long endTime = 0;
        try {

            startTime = System.currentTimeMillis();
            int i=0;
            while((System.currentTimeMillis()-startTime)<=60000) {
                i++;
                JSONObject record = new JSONObject();
                record.put("message_no", i);
                record.put("Time", System.currentTimeMillis());
                producer.send(new ProducerRecord<String, String>(topicName, "" + i, record.toString()));
            }
            System.out.println("Message sent successfully");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        producer.close();
        sparkJob.jssc.close();
        endTime = System.currentTimeMillis();
        System.out.println("Accumulator: "+sparkJob.getAccumulator().intValue()/(endTime - startTime)*1000);


        Thread.sleep(10000);


    }



    /**
     * shutdown ZK and Kafka Broker.
     */
    @After
    public void tearDown() {
        sparkJob.jssc.stop();
        kafka.shutdown();
        try {
            htu.shutdownMiniZKCluster();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
