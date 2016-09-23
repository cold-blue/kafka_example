/**
 * Created by yuhanliu on 9/22/2016.
 */

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerTest implements Runnable{
    private ConsumerConnector consumerConnector;
    private KafkaStream<byte[], byte[]> stream;
    private int threadNumber;
    private  long delay;

    public ConsumerTest(ConsumerConnector consumerConnectorOut,
                        KafkaStream<byte[], byte[]> streamOut,
                        int threadNumberOut, long delayOut) {
        consumerConnector = consumerConnectorOut;
        threadNumber = threadNumberOut;
        stream = streamOut;
        delay = delayOut;
    }

    // override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while(it.hasNext()) {
            String msg = new String(it.next().message());
            long time = System.currentTimeMillis() -
                    Long.parseLong(msg.substring(0, msg.indexOf(",")));
            if (time < delay) {
                try {
                    Thread.currentThread().sleep(delay - time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(System.currentTimeMillis() + ",Thread " +
                    threadNumber + ": " + msg);
        }
        System.out.println("Shutting down Thread: " + threadNumber);
    }

    public void run0() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        while (it.hasNext()) {
            //格式： 处理时间,线程编号:消息

            System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + new String(it.next().message()));
        }

        System.out.println("Shutting down Thread: " + threadNumber);

    }


    public void run1() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        int count = 0;
        while (it.hasNext()) {
            //格式： 处理时间,线程编号:消息
            System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + new String(it.next().message()));
            count++;

            if (count == 100) {
                consumerConnector.commitOffsets();
                count = 0;
            }
        }

        System.out.println("Shutting down Thread: " + threadNumber);

    }
}
