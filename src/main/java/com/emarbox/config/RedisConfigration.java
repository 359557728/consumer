package com.emarbox.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
@RefreshScope
public class RedisConfigration {

	@Value("${emar.bs.consumer.redis.info}")
	private String redisInfo;

	@Value("${redis.max.active}")
	private int maxActive;

	@Value("${redis.max.idle}")
	private int maxIdle;

	@Value("${redis.max.wait}")
	private int maxWait;

	@Value("${redis.connect.timeout}")
	private int timeout;

	@Value("${redis.retry.max.times}")
	private int maxRetryTimes;

	@Value("${redis.retry.initial.interval}")
	private Long initialRetryInterval;

	/**
	 * @return Map key is jedis pool, value is adx push data to this redis address.
	 */
	@Bean
	@RefreshScope
	public Map<JedisPool, List<String>> jedisPoolMap() {
		JedisPoolConfig jedisConfig = new JedisPoolConfig();
		jedisConfig.setMaxIdle(maxIdle);
		jedisConfig.setMaxWaitMillis(maxWait);
		jedisConfig.setTestOnBorrow(true);
		jedisConfig.setMaxTotal(maxActive);
		return Pattern.compile(",")
					.splitAsStream(redisInfo)
					.map(supplierIpPort -> Pattern.compile("-").splitAsStream(supplierIpPort).collect(Collectors.toList()))
					.filter(adxIpPortGroup -> adxIpPortGroup.size() == 3)
					.collect(Collectors.groupingBy(adxIpPortGroup -> adxIpPortGroup.get(1) + "," + Integer.parseInt(adxIpPortGroup.get(2)), Collectors.toList()))
					.entrySet()
					.stream()
					.collect(Collectors.toMap(adxIpPortGroup -> new JedisPool(jedisConfig, adxIpPortGroup.getKey().split(",")[0], Integer.parseInt(adxIpPortGroup.getKey().split(",")[1]), timeout), adxIpPortGroup -> adxIpPortGroup.getValue().stream().map(adxIpPortCol -> adxIpPortCol.get(0)).collect(Collectors.toList())));
	}

	@Bean
	@RefreshScope
	public RetryTemplate retryTemplate() {
		RetryTemplate template = new RetryTemplate();
		SimpleRetryPolicy policy = new SimpleRetryPolicy(maxRetryTimes, Collections.<Class<? extends Throwable>, Boolean>singletonMap(Exception.class, true));
		template.setRetryPolicy(policy);
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(initialRetryInterval);
		template.setBackOffPolicy(backOffPolicy);
		return template;
	}

}
