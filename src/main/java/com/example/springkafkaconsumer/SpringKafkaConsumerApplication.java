package com.example.springkafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
@Slf4j
public class SpringKafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaConsumerApplication.class, args);
	}

	@KafkaListener(topics = "spring-topic")
	public void listenToMessages(String data) {
		log.info("Mensaje recibido: '{}'", data);
	}

}
