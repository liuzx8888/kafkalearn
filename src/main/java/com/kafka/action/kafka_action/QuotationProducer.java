package com.kafka.action.kafka_action;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
		StockQuotationInfo quotationInfo = new StockQuotationInfo();
		Random r = new Random();
		Integer stockCode = 60010 + r.nextInt(10);
		float random = (float) Math.random();
		if (random / 2 < 0.5) {
			random = -random;
		}

		DecimalFormat decimalFormat = new DecimalFormat(".00");
		quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + random)));
		quotationInfo.setPreClosePrice(11.80f);
		quotationInfo.setLowPrice(10.5f);
		quotationInfo.setHighPrice(11.5f);
		quotationInfo.setStockCode(stockCode.toString());
		quotationInfo.setStockName("股票-" + stockCode);
		quotationInfo.setTradeTime(System.currentTimeMillis());
		return quotationInfo;

	}

	public static void main(String[] args) {
		ProducerRecord<String, String> record = null;
		StockQuotationInfo quotationInfo = null;
       
		try {
		int num = 0;
		for (int i = 0; i < MSG_SIZE; i++) {
			quotationInfo = createQuotationInfo();
			record = new ProducerRecord<String, String>(TOPIC, null, quotationInfo.getTradeTime(),
					quotationInfo.getStockCode(), quotationInfo.toString());
            producer.send(record);
            if (num++ % 10 ==0) {
            	Thread.sleep(2000L);
            }
            
		}
		}catch (InterruptedException e) {
			// TODO: handle exception
			LOG.error("Send message occurs exception", e);
		}finally {
			producer.close();
		}

	}

}
