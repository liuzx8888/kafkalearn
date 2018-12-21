package com.kafka.action.kafka_action;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class QuotationProducer {
	private static final Logger LOG = Logger.getLogger(QuotationProducer.class);
	/* 设置消息生产总数 */
	private static final int MSG_SIZE = 100;
	/* 主题名称 */
	private static final String TOPIC = "stock-quotation";
	/* kafka集群 */
	private static final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";
	private static KafkaProducer<String, String> producer = null;
	static {
		// 1.构建用于实例化KafkaProducer 的 properties 信息
		Properties configs = initconfig();

		// 2.初始化一个KafkaProducer
		producer = new KafkaProducer<String, String>(configs);
	}

	/* 初始化kafka设置 */
	private static Properties initconfig() {
		// TODO Auto-generated method stub
		Properties properties = new Properties();

		/* kafka broker 列表 */
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);

		/* 设置序列化 */
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;

	}

	/* 生产股票行情信息 */
	private static StockQuotationInfo createQuotationInfo() {
		StockQuotationInfo  quotationInfo  =new StockQuotationInfo();
		Random r = new Random();
		Integer stockCode = 60010+r.nextInt(10);
		float  random =  (float) Math.random();
		if(random/2<0.5) {
			random= -random;
		}
		return quotationInfo;
		
	}

}
