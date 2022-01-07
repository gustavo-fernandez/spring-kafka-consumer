package com.example.springkafkaconsumer;

import com.example.avro.model.Customer;
import com.example.springkafkaconsumer.util.CustomerDeserializer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

@SpringBootApplication
@Slf4j
public class SpringKafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaConsumerApplication.class, args);
	}

	@PostConstruct
	public void postConstruct() {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class);
		// config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		// config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Person.class);

		var receiverOptions = ReceiverOptions.<String, Customer>create(config)
			.subscription(List.of("spring-3-topic"));

		var kafkaReceiver = KafkaReceiver.create(receiverOptions);

		Flux<ReceiverRecord<String, Customer>> kafkaFlux = kafkaReceiver.receive()
			.doOnSubscribe(s -> log.info("-- Client subscribed"));

		kafkaFlux.subscribe(response -> {
			log.info("Mensaje recibido: \nkey: {}\nvalue: {}\nheaders: {}",
				response.key(), response.value(), response.headers());
		});
	}

}
