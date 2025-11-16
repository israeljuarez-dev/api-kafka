package com.api_kafka.config;

import com.api_kafka.listeners.ApiKafkaConsumerListener;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.HashMap;
import java.util.Map;

// Clase de configuración de consumer
@Configuration
public class StringConsumerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringConsumerConfig .class);

    @Autowired
    private KafkaProperties kafkaProperties;

    // configuracion de propiedades del consumer
    @Bean
    public ConsumerFactory<String, String> consumerFactory(){
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG    , StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> validMessageContainerFactory(ConsumerFactory<String, String> consumerFactory){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordInterceptor(validMessage());
        return factory;
    }

    private RecordInterceptor<String, String> validMessage(){
        return (record, consumer) -> {
            if (record.value().contains("hola")){
                LOGGER.info("Su mensaje contiene la palabra suscribete");
            }

            return record;
        };
    }
}
