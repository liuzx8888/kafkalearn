package com.kafka.action.kafka_action;

import java.io.InputStream;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface Consumer {
	static Properties initProperties() {
		return null;
	}

	static void subscribeTopicAuto() {
	};

	static void subscribeTopicCustom() {
	};

	static void subscribeTopicPartition() {
	};

	static void subscribeTopicTimestamp(KafkaConsumer<String, String> consumer, String topic, int... partitions) {
	};

	static void ConsumerTopicMessage(KafkaConsumer<String, String> consumer) {
	};

	static InputStream MsgsToHdfs(KafkaConsumer<String, String> consumer) {
		return null;
	};
}
