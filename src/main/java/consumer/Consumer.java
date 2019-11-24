package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;


public class Consumer {
    private static final String TOPIC = "test";

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(setConsumerProps());
        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//            consumer.seekToBeginning(records.partitions());
//            consumer.seekToEnd(records.partitions());
//            consumer.seek(new TopicPartition(TOPIC, 0), 20L);
            records.forEach(record ->
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
            consumer.commitSync();
//            consumer.commitAsync();

//          Если коммит оффсетов зафейлится, то это будет залогировано
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition,
                                        OffsetAndMetadata> offsets, Exception e) {
                    if (e != null)
//                        log.error("Commit failed for offsets {}", offsets, e);
                        System.out.printf("Commit failed for offsets %s", offsets);
                }
            });
        }
    }

    private static Properties setConsumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
