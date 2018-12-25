package com.kafka.action.kafka_action;

<<<<<<< HEAD
import java.util.ArrayList;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
=======
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
>>>>>>> refs/remotes/origin/master

/**
 * Hello world!
 *
 */
<<<<<<< HEAD
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

=======
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";

        System.out.println(Arrays.asList(BROKER_LIST.split(",")));
        
        System.out.println(Arrays.asList(StringUtils.split(BROKER_LIST, ",")));
    }
>>>>>>> refs/remotes/origin/master
}
