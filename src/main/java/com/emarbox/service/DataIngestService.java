package com.emarbox.service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.emarbox.config.CommonConfigration;
import com.emarbox.config.IntegrationConfigration.FileLogGateway;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
public class DataIngestService {

	private static final String lineSeparator = System.getProperty("line.separator");

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final Logger throughput = LoggerFactory.getLogger("throughput");

	@Autowired
	private KafkaReceiver<String, String> kafkaDataReceiver;

	@Autowired
	private Map<JedisPool, List<String>> jedisPoolMap;

	@Autowired
	private FileLogGateway fileLogGateway;

	@Autowired
	private RetryTemplate retryTemplate;

	@Autowired
	private CommonConfigration commonConfigration;

	public void kafkaDataIngest() {
		Flux<ReceiverRecord<String, String>> inboundFlux = kafkaDataReceiver.receive();
		inboundFlux
		.groupBy(record -> record.receiverOffset().topicPartition())
		.flatMap(records -> records
			.bufferTimeout(commonConfigration.getBufferSize(), Duration.ofSeconds(commonConfigration.getBufferTime()))
			.publishOn(Schedulers.elastic())
			.map(recordbatch -> 
				{
					String offset = "";
					try {
						offset = processRecord(recordbatch);
					} catch (Exception e) {
						log.error("record process failed in stream, caused by ", e);
					}
					return offset;
				}
			))
		.subscribe(
			offset -> {
				throughput.info("offset-partion-offset {} committed", offset);
			},
			exception -> {
				log.error("error occured when handle kafka message, caused by " + exception);
			}
		);
	}

	private String processRecord(List<ReceiverRecord<String, String>> reocrds) throws Exception {
		StringBuffer commitResult = new StringBuffer();
		AtomicLong counter = new AtomicLong(0);
		retryTemplate.execute(
			context -> {
				String topic = reocrds.get(0).topic();
				Optional<Map.Entry<JedisPool, List<String>>> firstMatchedPool = jedisPoolMap.entrySet().stream().filter(entry -> entry.getValue().contains(topic)).findFirst();
				if(firstMatchedPool.isPresent()) {
					Jedis jedis = null;
					Long result = null;
					try {
						JedisPool jedisPool = firstMatchedPool.get().getKey();
						jedis = jedisPool.getResource();
						long timeStart = System.nanoTime();
						String[] bufferedMessage = reocrds.stream().map(record -> record.value()).toArray(String[]::new);
						jedis.rpush(commonConfigration.getRedisKey(), bufferedMessage);
						double usedTime = (double)(System.nanoTime() - timeStart) / 1000000.0;
						ReceiverRecord<String, String> fistRecord = reocrds.get(0);
						ReceiverRecord<String, String> lastRecord = reocrds.get(reocrds.size() - 1);
						result = lastRecord.offset();
						counter.addAndGet(result);
						lastRecord.receiverOffset().commit();
						throughput.info("usedTime {} size {} realsize {} beginoffset {} offset {} topicbatch {} topic {} partition {}", 
								usedTime, bufferedMessage.length, reocrds.size(), fistRecord.offset(), lastRecord.offset(), 
								topic, lastRecord.topic(), lastRecord.partition());
						commitResult.append(topic).append("-").append(lastRecord.partition()).append("-").append(counter.get());
					} catch (Exception e) {
						if(jedis != null) {
							jedis.close();
						}
						log.error("record process failed, caused by ", e);
						throw new Exception("error ocurred when retrieve jedis");
					} finally {
						if(jedis != null) {
							jedis.close();
						}
					}
				}
				return true;
			}, 
			context -> {
				context.getLastThrowable().printStackTrace();
				String logText = reocrds.stream().map(flatLog -> reocrds.get(0).topic() + commonConfigration.getRecordSeparator() + flatLog.value() + lineSeparator).collect(Collectors.joining(""));
				fileLogGateway.writeToFile(logText);
				return true;
			}
		);
		return commitResult.toString();
	}
}
