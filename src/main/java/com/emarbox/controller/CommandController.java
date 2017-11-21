package com.emarbox.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.emarbox.config.IntegrationConfigration.CommandReplyGateway;
import com.emarbox.config.IntegrationConfigration.CommandSendGateway;

@RestController
@RequestMapping(value = "/command")
public class CommandController {

	@Autowired
	private CommandSendGateway commandSendGateway;

	@Autowired
	private CommandReplyGateway commandReplyGateway;

	@RequestMapping(value = "/consumer/start")
	public void billingResendStart() {
		commandSendGateway.send("@consumerResend.start()");
	}

	@RequestMapping(value = "/consumer/stop")
	public void billingResendStop() {
		commandSendGateway.send("@consumerResend.stop()");
	}

	@RequestMapping(value = "/consumer/status")
	public String rtbResendStatus() {
		return commandReplyGateway.send("@consumerResend.isRunning()");
	}

}
