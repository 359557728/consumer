package com.emarbox.config;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Configuration
public class KafkaReceiverConfigration {

	@Value("${emar.bs.consumer.metadata.broker.list}")
	private String brokerList;

	@Value("${emar.bs.consumer.group.name}")
	private String groupName;

	@Value("${metadata.max.age.ms}")
	private Long metadataAge;

	@Value("${metadata.session.timeout.ms}")
	private Integer timeOut;

	@Autowired
	private CommonConfigration commonConfigration;

	@Bean
	public KafkaReceiver<String, String> kafkaDataReceiver() {
		Map<String, Object> consumerProps = new HashMap<>();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metadataAge);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, timeOut);
		consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, timeOut.intValue() + 1000);
		ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(consumerProps)
				.subscription(
						Pattern.compile(",").splitAsStream(commonConfigration.getTopicList())
								.collect(Collectors.toList()));
		return KafkaReceiver.create(receiverOptions);
	}
}
