package com.kafka.action.kafka_action;

import java.util.ArrayList;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Hello world!
 *
 */
public class App {

	private static void subscribeTopicPartition(int... partitions) {
		ArrayList<TopicPartition> Topicpartition = new ArrayList<TopicPartition>();
		for (int partitionId : partitions) {
				Topicpartition.add(new TopicPartition("stock-quotation", partitionId));
		}
		System.out.println(Topicpartition);
	}

	public static void main(String[] args) {
		System.out.println("Hello World!");
		App.subscribeTopicPartition(1,2);
	}

}
