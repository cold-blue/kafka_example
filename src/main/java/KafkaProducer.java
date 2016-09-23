/**
 * Created by yuhanliu on 9/22/2016.
 */
import java.util.Date;
import  java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class KafkaProducer extends Thread {

    private String topic;
    String brokers;
    //Random random;


    public KafkaProducer(String topicOut, String brokersOut)
    {
        super();
        topic = topicOut;
        brokers = brokersOut;
        //random = new Random();
    }

    //override
    public void run() {
        Producer producer = createProducer();
        int index = 0;
        int count = 10;
        while((count --) != 0) {
            producer.send(new KeyedMessage(topic, "message " + index));
            index ++;
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }


    private Producer createProducer() {
        Properties properties = new Properties();
//        properties.put("zookeeper.connect",
//                "192.168.150.112.0:2181, 192.168.150.112.1:2181, 192.168.150.112.2:2181");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", brokers);
        properties.put("producer.type", "async");
        return new Producer<Integer, String>(new ProducerConfig(properties));
    }

    public static void main(String args[]) {
        //long events = Long.parseLong(args[0]);
        String topic = args[0];
        String brokers = args[1];

        new KafkaProducer(topic, brokers).start();
    }

}
