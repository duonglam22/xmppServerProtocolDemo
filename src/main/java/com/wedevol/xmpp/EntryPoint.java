package com.wedevol.xmpp;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jivesoftware.smack.XMPPException;

import com.wedevol.xmpp.bean.CcsOutMessage;
import com.wedevol.xmpp.server.CcsClient;
import com.wedevol.xmpp.server.MessageHelper;
import com.wedevol.xmpp.util.Util;

/**
 * Entry Point class for the XMPP Server in dev mode for debugging and testing
 * purposes
 */
public class EntryPoint {
	
	public static final Logger logger = Logger.getLogger(EntryPoint.class.getName());
	
	public static void main(String[] args) {
//		final String fcmProjectSenderId = args[0];
//		final String fcmServerKey = args[1];
//		final String toRegId = args[2];

//		String fcmProjectSenderId = "messagecloudexample";
		String fcmProjectSenderId = "97098250087";
		String fcmServerKey = "AAAAFpuBt2c:APA91bEtr3dDnEooTASU_W24bn13d2G0W_VXOXXLSJSJxSZpLNKxQEENYl-mgr-Juap2U8Qj61C8WEVEbKBYl7gLJFwE9RIRVJXMXuncF3MdhBOTmp-sfQB4OPRI2N4vFFkDuf9SZROd";
		String toRegId = "registerId";

		CcsClient ccsClient = CcsClient.prepareClient(fcmProjectSenderId, fcmServerKey, true);

		try {
			ccsClient.connect();
		} catch (XMPPException e) {
			logger.log(Level.SEVERE, "Error trying to connect.", e);
		}

		// Send a sample downstream message to a device
//		String messageId = Util.getUniqueMessageId();
//		Map<String, String> dataPayload = new HashMap<String, String>();
//		dataPayload.put(Util.PAYLOAD_ATTRIBUTE_MESSAGE, "This is the simple sample message");
//		CcsOutMessage message = new CcsOutMessage(toRegId, messageId, dataPayload);
//		String jsonRequest = MessageHelper.createJsonOutMessage(message);
//		ccsClient.send(jsonRequest);

		Process process = new Process(ccsClient);
		int numberOfMsg;
		Scanner scanner = new Scanner(System.in);
		System.out.println("input number of msg: ");
		numberOfMsg = scanner.nextInt();

		process.fillNotificationQueue(numberOfMsg);
//		process.fillDataQueue(numberOfMsg);
		process.start();
	}
}
