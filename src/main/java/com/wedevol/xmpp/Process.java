package com.wedevol.xmpp;

import com.wedevol.xmpp.bean.CcsOutMessage;
import com.wedevol.xmpp.server.CcsClient;
import com.wedevol.xmpp.server.MessageHelper;
import com.wedevol.xmpp.util.Util;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

public class Process extends Thread {
    private static final Logger logger = Logger.getLogger(Process.class.getName());
    Queue<CcsOutMessage> mQueue = new ArrayDeque<>(400000);
    CcsClient ccsClient = null;

    public Process(CcsClient ccsClient) {
        this.ccsClient = ccsClient;
    }
    public void fillDataQueue(int size) {
        for(int i = 1; i <= size; ++i) {
            String toRegId = "registerId-" + i;
            String messageId = Util.getUniqueMessageId();
            Map<String, String> dataPayload = new HashMap<String, String>();
            dataPayload.put(Util.PAYLOAD_ATTRIBUTE_MESSAGE, "This is the simple sample message" + i);
            CcsOutMessage message = new CcsOutMessage(toRegId, messageId, dataPayload);

            mQueue.add(message);
        }
        logger.info("fill queue with number of element: " + mQueue.size());
    }

    public void fillNotificationQueue(int size) {
        for(int i = 1; i <= size; ++i) {
            String toRegId = "/topics/" + i%4000;
            String messageId = Util.getUniqueMessageId();
            Map<String, String> notiPayload = new HashMap<String, String>();
            notiPayload.put(Util.TITLE_ATTRIBUTE_NOTIFICATION, "vnpt-pay-" + i);
            notiPayload.put(Util.BODY_ATTRIBUTE_NOTIFICATION, "this example noti to test");
            CcsOutMessage message = new CcsOutMessage(toRegId, messageId, notiPayload);

            mQueue.add(message);
        }
        logger.info("fill queue with number of element: " + mQueue.size());
    }

    @Override
    public void run() {
        logger.info("begin dequeue");
        while (true) {
            logger.info("size of msg queue: " + mQueue.size());

            if(mQueue.isEmpty()) {
                logger.info("message queue is empty");
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else {
                CcsOutMessage message = mQueue.poll();
                String jsonRequest = MessageHelper.createJsonOutMessage(message);
		        ccsClient.send(jsonRequest);
            }
        }
    }
}
