package org.apache.spark.examples.streaming;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
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



    /**
     * start ZK and Kafka Broker.
     */
    @Before
    public void initialize() {
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
    }

    /**
     * test directory mode of the trip generator.
     */
    @Test
    public void someKafkaTest() {

        try {
            SparkDrizzleStreamingJSONJob.main(new String[]{
                    zookeeperConnect, "my-consumer-group", "test", "1"

            });



        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }


    }



    /**
     * shutdown ZK and Kafka Broker.
     */
    @After public void tearDown() {
        kafka.shutdown();

        try {
            htu.shutdownMiniZKCluster();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
