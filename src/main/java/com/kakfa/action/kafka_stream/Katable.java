package com.kakfa.action.kafka_stream;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import kafka.consumer.Consumer;

public class Katable extends Thread {

	private static Properties pops = null;
	private static final String BROKER_LIST = "192.168.1.70:9092,192.168.1.71:9092,192.168.1.72:9092,192.168.1.73:9092";

	public static Properties initconfig() {
		pops = new Properties();
		// 指定流处理的id，必须要指定
		pops.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kstream-test1");
		pops.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

		// Key 序列化 与反序列化
		pops.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// Value 序列化 与反序列化
		pops.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		pops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return pops;

	}

	public static void main(String[] args) {
		pops = Katable.initconfig();
		KStreamBuilder builder = new KStreamBuilder();
		// 构建 kTable 日志流
		KTable<String, String> kTable = builder.table("streams-foo", "kTable-test");

		// 输入日志流数据
		kTable.print();

		KafkaStreams kafkaStreams = new KafkaStreams(builder, pops);
		kafkaStreams.start();

		// 线程睡眠5S

		try {
			Thread.sleep(5000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		kafkaStreams.close();
	}

}
