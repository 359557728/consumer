package com.emarbox.config;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.support.Transformers;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.filters.LastModifiedFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.transformer.Transformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.retry.support.RetryTemplate;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Configuration
@IntegrationComponentScan
public class IntegrationConfigration {

	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private static final String lineSeparator = System.getProperty("line.separator");

	private final Logger throughput = LoggerFactory.getLogger("throughput");

	@Value("${emar.log.directory}")
	private String logDirectory;

	@Value("${emar.log.format}")
	private String logFormat;

	@Autowired
	private Map<JedisPool, List<String>> jedisPoolMap;

	@Autowired
	private CommonConfigration commonConfigration;

	@Autowired
	private RetryTemplate retryTemplate;

	@Autowired
	private FileLogGateway fileLogGateway;

	@Bean
   	public IntegrationFlow fileWritingFlow() {
   	    return IntegrationFlows.from("flowToFileChannel")
   		        .enrichHeaders(h -> h.header("directory", new File(logDirectory)))
   	            	.handleWithAdapter(a -> a.file(m -> m.getHeaders().get("directory")).fileExistsMode(FileExistsMode.APPEND).autoCreateDirectory(true)
   	            		.fileNameGenerator(m -> {
   	            			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh-mm");
   	            			String date = dateFormat.format(new Date()); 
   	            			return date + "." + logFormat;
   	            		})
			).get();
    	}

	@MessagingGateway(defaultRequestChannel = "flowToFileChannel")
	public interface FileLogGateway {
		void writeToFile(String data);
	}

	@Bean
   	public IntegrationFlow consumerResendFileReadingFlow() {
    		return IntegrationFlows
    			.from(sources -> sources.file(new File(logDirectory)).filter(fileFilter()).autoCreateDirectory(true), 
    				  spec -> spec.poller(Pollers.fixedRate(1, TimeUnit.MINUTES).maxMessagesPerPoll(1)).autoStartup(true).id("consumerResend"))
    			.transform(fileToStringTransformer())
    			.split(spec -> spec.applySequence(false).get().getT2().setDelimiters(lineSeparator))
    			.filter((String source) -> !"".equals(source.trim()) && source.contains(commonConfigration.getRecordSeparator()))
    			.aggregate(a -> a.correlationStrategy(m -> m.getPayload().toString().split(commonConfigration.getRecordSeparator())[0])
    					.outputProcessor(g -> MessageBuilder.withPayload(g.getMessages().stream().<String>map(m -> m.getPayload().toString()
    							.split(commonConfigration.getRecordSeparator())[1]).collect(Collectors.joining(commonConfigration.getRecordSeparator()))).setHeader("correlationId", g.getOne().getPayload().toString().split(commonConfigration.getRecordSeparator())[0]).build())
    					.releaseStrategy(g -> g.size() > commonConfigration.getBatchWriteSize())
    					.groupTimeout(g -> g.size() * 10L < 2000 ? 2000 : g.size() * 10L)
    					.expireGroupsUponCompletion(true)
    					.expireGroupsUponTimeout(true)
    					.sendPartialResultOnExpiry(true))
    			.handle(this::messageReProcess)
    			.get();
    	}

	private void messageReProcess(Message<?> message) {
    		String topic = message.getHeaders().get("correlationId").toString();
    		String reocrds = message.getPayload().toString();
    		String[] dataBatch = Pattern.compile(commonConfigration.getRecordSeparator()).splitAsStream(reocrds).toArray(String[]::new);
    		if(StringUtils.isNotBlank(topic) && StringUtils.isNotBlank(reocrds) && commonConfigration.getTopicList().contains(topic)) {
    			throughput.info("records withs topic {} message size {} resend start", topic, dataBatch.length);
    			try {
				retryTemplate.execute(
					context -> {
						Optional<Map.Entry<JedisPool, List<String>>> firstMatchedPool = jedisPoolMap.entrySet().stream().filter(entry -> entry.getValue().contains(topic)).findFirst();
						if(firstMatchedPool.isPresent()) {
							Jedis jedis = null;
							JedisPool jedisPool = firstMatchedPool.get().getKey();
							try {
								jedis = jedisPool.getResource();
								jedis.rpush(commonConfigration.getRedisKey(), dataBatch);
							} catch (Exception e) {
								log.error("resend error, caused by " + e.getMessage());
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
						String logText = Arrays.stream(dataBatch).map(flatLog -> topic + commonConfigration.getRecordSeparator() + flatLog + lineSeparator).collect(Collectors.joining(""));
						fileLogGateway.writeToFile(logText);
						return true;
					}
				);
				throughput.info("records withs topic {} message size {} resend end", topic, dataBatch.length);
			} catch (Exception e) {
				throughput.error("records withs topic {} message size {} resend error {}", topic, dataBatch.length, e);
			}
    		}
   	}

	@Bean
	public ChainFileListFilter<File> fileFilter() {
		ChainFileListFilter<File> chainFileListFilter = new ChainFileListFilter<File>();
		SimplePatternFileListFilter simplePatternFileListFilter = new SimplePatternFileListFilter("*." + logFormat);
		LastModifiedFileListFilter lastModifiedFileListFilter = new LastModifiedFileListFilter();
		lastModifiedFileListFilter.setAge(70, TimeUnit.SECONDS);
		chainFileListFilter.addFilter(simplePatternFileListFilter);
		chainFileListFilter.addFilter(lastModifiedFileListFilter);
		return chainFileListFilter;
	}

	@Bean
	public Transformer fileToStringTransformer() {
		FileToStringTransformer fileToStringTransformer = Transformers.fileToString();
		fileToStringTransformer.setDeleteFiles(true);
		return fileToStringTransformer;
	}

	/**
	 * control bus used to receive the command send by below gateway
	 * @return
	 */
	@Bean
	public IntegrationFlow controlBusFlow() {
		return IntegrationFlows.from("commandChannel").controlBus().get();
	}

	@MessagingGateway
	public interface CommandSendGateway {
		@Gateway(requestChannel = "commandChannel")
		void send(String command);
	}

	@MessagingGateway
	public interface CommandReplyGateway {
		@Gateway(requestChannel = "commandChannel")
		String send(String command);
	}

	@Bean
	public MessageHandler logger() {
		LoggingHandler loggingHandler = new LoggingHandler(LoggingHandler.Level.INFO.name());
		loggingHandler.setLoggerName("wiretap");
		return loggingHandler;
	}
}
