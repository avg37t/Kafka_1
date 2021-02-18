package by.epam.kafka.task1.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import java.util.stream.Stream;

import static by.epam.kafka.task1.constant.Constant.*;

public class Producer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_VALUE);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Stream.iterate(new int[]{0, 1}, arr -> new int[]{arr[1], arr[0] + arr[1]})
                .limit(Integer.parseInt(args[0]))
                .map(element -> String.valueOf(element[0]))
                .forEach(element -> {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, element);
                    producer.send(record, (recordMetadata, e) -> {
                        if (e == null) {
                            logger.info(HEADER +
                                    TOPIC_STR + recordMetadata.topic() + "\n" +
                                    PARTITION + recordMetadata.partition() + "\n" +
                                    OFFSET + recordMetadata.offset() + "\n" +
                                    TIMESTAMP + recordMetadata.timestamp());
                        } else logger.error(ERR_PR, e);
                    });
                    producer.flush();
                });
        producer.close();
    }
}
