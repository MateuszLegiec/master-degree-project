import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import pl.pp.noc.commons.app.kafka.purde;

public class CustomKafkaProducer {

    private static Map.Entry<Map<String, String>, String> of(Map<String, String> a, String b) {
        return new AbstractMap.SimpleEntry<>(a, b);
    }

    public static void main(String[] args) {
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient.create(adminProperties).listTopics().names().thenApply(it -> {
            System.out.println(it);
            return it;
        });



//        DynamicProducer dynamicProducer = new DynamicProducer();
//        IntStream.range(1, 3)
//                .mapToObj(it -> of(Map.of("TOPIC", String.valueOf(it)), "EVENT_" + it))
//                .parallel()
//                .forEach(it -> IntStream.range(0, 100000)
//                        .forEach(i -> {
//                            try {
//                                Thread.sleep(1000);
//                                dynamicProducer.send(it.getKey(), it.getValue(), it.getValue() + "_" + i);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        })
//                );
//        Set.of(
//                        of(Map.of("CASES", "1234", "CREDITOR", "123"), LocalDateTime.now().toString()),
//                        of(Map.of("CASES", "1234"), LocalDateTime.now().toString()),
//                        of(Map.of("CASES", "123456", "CREDITOR", "123"), LocalDateTime.now().toString())
//                )
//                .forEach(it -> dynamicProducer.send(it.getKey(), it.getValue()));
    }

}

class PurdeProducer {

    PurdeMessageNotReceivedKafkaMessage messageNotReceivedKafkaMessage;

}

class DynamicProducer {

    private final Producer<Object, Object> producer;
    private final AdminClient adminClient;
    private final static Function<Map.Entry<String, String>, String> TOPIC_NAME_BUILDER = entry -> entry.getKey() + "_" + entry.getValue();

    public DynamicProducer() {
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(producerProperties);
        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        this.adminClient = AdminClient.create(adminProperties);
    }


    public void send(Map<String, String> map, String key, String value) {
        map.entrySet()
                .stream()
                .map(TOPIC_NAME_BUILDER)
                .parallel()
                .forEach(
                        topicName -> {
                            try {
                                createTopicIfNotExists(topicName)
                                        .whenComplete((list, err) -> {
                                            if (err == null) {
                                                System.out.println("sending message: " + value + " to topic: " + topicName);
                                                producer.send(new ProducerRecord<>(topicName, key, value));
                                            } else {
                                                err.printStackTrace();
                                            }
                                        }).get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                );
    }

    private KafkaFuture<Set<String>> createTopicIfNotExists(String topicName) {
        return adminClient.listTopics().names()
                .thenApply(listTopics -> {
                            boolean contains = listTopics.contains(topicName);
                            if (!contains) {
                                List<NewTopic> topicList = new ArrayList<>();
                                Map<String, String> configs = new HashMap<>();
                                int partitions = 5;
                                short replication = 1;
                                NewTopic newTopic = new NewTopic(topicName, partitions, replication).configs(configs);
                                topicList.add(newTopic);
                                System.out.println("creating topic with name: " + topicName);
                                adminClient.createTopics(topicList);
                                return Stream.concat(listTopics.stream(), Stream.of(topicName))
                                        .collect(Collectors.toSet());
                            }
                            return listTopics;
                        }
                );
    }

}
