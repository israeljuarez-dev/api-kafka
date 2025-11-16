package com.api_kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    // Necesitamos una instancia de KafkaProperties para realizar la configuración,
    // nos brinda las propieades definidas en el yml
    @Autowired
    private KafkaProperties kafkaProperties;

    // KafkaAdmin nos facilita la configuración y adminsitración de recursos en kafka
    // Le indicamos BOOTSTRAP_sERVER, es un servidor de arranque
    // son una loista de seguidores en kafka al cual elcliente se conectará
    // siempre necesitaremos un servidor en kafka
    // Configuración de servidor de arranque
    @Bean
    public KafkaAdmin kafkaAdmin(){
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(configs);
    }

    // Configuración de topics
    @Bean
    public KafkaAdmin.NewTopics topics(){
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("api-topic")
                        .partitions(2) // este topic tendrá dos particiones
                        .replicas(1) // 1 replica por cada particion
                        .build()
        );
    }
}
