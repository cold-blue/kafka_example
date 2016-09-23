/**
 * Created by yuhanliu on 9/22/2016.
 */
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer extends Thread{

    private String topic;
    String zookeeper;
    String groupId;
    private long delay;

    private ConsumerConnector myConsumerConnector;
    private ExecutorService executorService;

    public void shutdown() {
        if (myConsumerConnector != null)
            myConsumerConnector.shutdown();
        if (executorService != null)
            executorService.shutdown();
    }

    public KafkaConsumer(String zookeeperOut,
                         String groupIdOut, String topicOut, long delayOut) {
        super();
        topic = topicOut;
        zookeeper = zookeeperOut;
        groupId = groupIdOut;
        delay = delayOut;
    }

    //override
    public void run(int numThreadsOut) {
        myConsumerConnector = createConsumerConnector();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreadsOut));
        Map<String, List< KafkaStream<byte[], byte[]> > > messageStreams =
                myConsumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]> > streams = messageStreams.get(topic);

        // commit to thread pool
        executorService = Executors.newFixedThreadPool(numThreadsOut);
        int threadNumber = 0;
        for(final KafkaStream<byte[], byte[]> stream : streams) {

            executorService.submit(
                    new ConsumerTest(myConsumerConnector, stream, threadNumber, delay));
            threadNumber++;
        }
    }

    private ConsumerConnector createConsumerConnector() {
        Properties properties1 = new Properties();
        properties1.put("zookeeper.connect", zookeeper);
        properties1.put("auto.offset.reset", "largest");
        properties1.put("group.id", groupId);
        properties1.put("zookeeper.session.timeout.ms", "400");
        properties1.put("zookeeper.sync.time.ms", "200");
        properties1.put("auto.commit.intervals.ms", "1000");

        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties1));
    }

    public static void main(String args[]) {
        String zookeeper = args[0];
        String groupId = args[1];
        String topic = args[2];

        int numThreads = Integer.parseInt(args[3]);

        long delay = Long.parseLong(args[4]);

        new KafkaConsumer(zookeeper, groupId, topic, delay).run(numThreads);
    }
}
