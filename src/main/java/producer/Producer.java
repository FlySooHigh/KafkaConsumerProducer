package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

public class Producer {
    private static final int NUMBER_OF_MESSAGES_TO_PRODUCE = 1;
    private static final String TOPIC = "test";

    public static void main(String[] args) {
        runProducer(setProducerProps());
    }

    private static void runProducer(Properties props) {
        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);
        IntStream.range(0, Producer.NUMBER_OF_MESSAGES_TO_PRODUCE)
                .forEach(num ->
                        producer.send(new ProducerRecord<>(TOPIC, Integer.toString(num), Integer.toString(num))));
        producer.close();
    }

    private static Properties setProducerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
