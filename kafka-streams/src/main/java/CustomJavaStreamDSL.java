import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.Set;

public class CustomJavaStreamDSL {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(Set.of("TOPIC_1", "TOPIC_2"));
        stream
                .peek((key, value) -> System.out.println(key + " : " + value))
                .map((key, value) -> new KeyValue<>(key, "{ \"" + key + "\" : \"" + value + "\" }"))
                .peek((key, value) -> System.out.println(key + " : " + value))
                .to("CREDITOR_3");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties());
        streams.start();

        Runtime.getRuntime()
                .addShutdownHook(new Thread(streams::close));
    }

    private static Properties properties(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "CUSTOM_JAVA_STREAM_PROCESSOR");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return properties;
    }

}
