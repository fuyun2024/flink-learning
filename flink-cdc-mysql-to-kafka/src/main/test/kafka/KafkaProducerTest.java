package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerTest {

    public static String topic = "UNICOM_SIGNAL_DECTION_FILE_NAME";//定义主题

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.152.128:9092");//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);


        String msg = "{\"id\":5,\"name\":\"hudi1\",\"price\":10,\"ts\":1000,\"dt\":\"20210101\"}";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
        kafkaProducer.send(record);
        System.out.println("消息发送成功:" + msg);
        kafkaProducer.close();


//        try {
//            while (true) {
//                String msg = "Hello," + new Random().nextInt(100);
//                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
//                kafkaProducer.send(record);
//                System.out.println("消息发送成功:" + msg);
//                Thread.sleep(500);
//            }
//        } finally {
//            kafkaProducer.close();
//        }



    }
}