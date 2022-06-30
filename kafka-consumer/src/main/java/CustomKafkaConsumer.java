import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

public class CustomKafkaConsumer {
    public static void main(String[] args) {
        final DynamicConsumer dynamicConsumer = new DynamicConsumer();
//        final Set<String> topicNamesStream = getStreamOfRandomNumbers(10, 5)
//                .map(it -> "CREDITOR_" + it)
//                .collect(Collectors.toSet());
        dynamicConsumer.readRecords(Collections.singleton("CREDITOR_3"))
                .forEach((topic, events) -> System.out.println(topic + " - " + events));
    }

    private static Stream<Integer> getStreamOfRandomNumbers(int range, int size) {
        final List<Integer> ints = IntStream.range(0, range).boxed().collect(Collectors.toList());
        Collections.shuffle(ints);
        return ints.stream().limit(size);
    }

}

class DynamicConsumer {

    public DynamicConsumer() {
    }

    Map<String, List<String>> readRecords(Set<String> topicNames) {
        Map<String, List<String>> topicNameToMessage = topicNames
                .stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                it -> new LinkedList<>())
                );
        final long startTimeInMillis = System.currentTimeMillis();
        topicNames
                .stream()
                .parallel()
                .forEach(it -> {
                    final Properties properties = new Properties();
                    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
                    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
                    properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 5 * ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);
                    properties.put(ConsumerConfig.GROUP_ID_CONFIG, it + "_" + UUID.randomUUID());
                    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
                    consumer.subscribe(Collections.singleton(it));
                    consumer.seekToBeginning(consumer.assignment());
                    ConsumerRecords<String, String> records;
                    do {
                        records = consumer.poll(Duration.ofMillis(5));
                        for (ConsumerRecord<String, String> record : records) {
                            topicNameToMessage.get(record.topic()).add(record.value());
                        }
                    } while (records.isEmpty());

                }
        );
        final long time = System.currentTimeMillis() - startTimeInMillis;
        System.out.println("END_TIME: " + time);
        return topicNameToMessage;
    }

}

class Test{

    private final static Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final static KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Properties());

    public static void main(String[] args) {
        int count = 0;
        Duration timeout = Duration.ofMillis(1000);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                currentOffsets.put(
                        new TopicPartition(
                                record.topic(),
                                record.partition()
                        ),
                        new OffsetAndMetadata(
                                record.offset()+1, "no metadata"
                        )
                );
                if (count % 10 == 0)
                    consumer.commitAsync(currentOffsets, null);
                count++;
            }
        }
    }

}
