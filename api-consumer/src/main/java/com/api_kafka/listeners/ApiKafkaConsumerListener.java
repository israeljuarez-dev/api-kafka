package com.api_kafka.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.stereotype.Component;

@Component
public class ApiKafkaConsumerListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiKafkaConsumerListener.class);

    @KafkaListener(
            groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "api-topic", partitions = {"0"}),
            containerFactory = "validMessageContainerFactory"
    )
    public void listener1(String message){
        LOGGER.info("LISTENER1 ::: recibiendo un mensaje: {}", message);
    }

    @KafkaListener(
            groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "api-topic", partitions = {"1"}),
            containerFactory = "validMessageContainerFactory"
    )
    public void listener2(String message){
        LOGGER.info("LISTENER2 ::: recibiendo un mensaje: {}", message);
    }

    // cuando no indicamos una partición en particular, utiliza todas las particiones
    @KafkaListener(
            groupId = "group-2",
            topics = "api-topic",
            containerFactory = "validMessageContainerFactory"
    )
    public void listener3(String message){
        LOGGER.info("LISTENER3 ::: recibiendo un mensaje: {}", message);
    }
}
