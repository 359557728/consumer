package com.emarbox;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.emarbox.service.DataIngestService;

@SpringBootApplication
@EnableEurekaClient
@EnableScheduling
public class ConsumerApplication {

	@Autowired
	private DataIngestService dataIngestService;

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

	@Bean
	CommandLineRunner runner(){
		return args -> {
			dataIngestService.kafkaDataIngest();
		};
	}
}
