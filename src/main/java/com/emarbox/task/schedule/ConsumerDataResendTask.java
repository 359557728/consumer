package com.emarbox.task.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * 
 * health checker
 *
 */
@Service
public class ConsumerDataResendTask {

	private final Logger log = LoggerFactory.getLogger("health");

	@Scheduled(cron = "${emar.bs.consumer.resend.cron}")
	public void xviewMeidaDataTransfer() {
		log.info("resend task begin");
		log.info("resend task end");
	}

}
