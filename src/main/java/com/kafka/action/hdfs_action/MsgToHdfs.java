package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.io.InputStream;

import com.kafka.action.kafka_action.KafkaNewConsumer;

public class MsgToHdfs {

	public static void main(String[] args) throws Exception {
		String topicName = "TAB";
		String path = "/" + topicName + System.currentTimeMillis();
		FsSystem fsSystem = new FsSystem();
		KafkaNewConsumer consumer = new KafkaNewConsumer();
		InputStream in = consumer.subscribeTopicCustom(consumer.kafkaConsumerconsumer, topicName);
		/* Boolean rs = fsSystem.mkdirs(fsSystem.FS, "/" + "topicName" + ".txt"); */
		fsSystem.outputdata(fsSystem.FS, in, path);
		/*
		 * in.close(); consumer.kafkaConsumerconsumer.close();
		 */
	}

}
