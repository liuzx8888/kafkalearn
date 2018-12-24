package com.kafka.action.kafka_action;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

public class KafkaProducerThread implements Runnable {

	private static final Logger LOG = Logger.getLogger(KafkaProducerThread.class);
	private static final int MSG_SIZE = 100;
	private static final int THREAD_NUM = 2;
	private static final String TOPIC = "stock-quotation";
/*	private static final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";*/
	private static final String BROKER_LIST = "192.168.1.70:9092,192.168.1.71:9092,192.168.1.72:9092,192.168.1.73:9092";	
	private static KafkaProducer<String, String> producer = null;

	KafkaProducer<String, String> kafkaProducer = null;
	ProducerRecord<String, String> record = null;
	static StockQuotationInfo quotationInfo = null;

	public KafkaProducerThread(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> record) {
		this.kafkaProducer = kafkaProducer;
		this.record = record;
	}

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

	@Override
	public void run() {
		// TODO Auto-generated method stub

		kafkaProducer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				System.out.println("111");
				if (exception != null) {
					LOG.error("Send message occurs exception", exception);
				}
				if (metadata != null) {
					System.out.println("222");
					LOG.info(String.format("offset:%s,partition:%s", metadata.offset(), metadata.partition()));
				}
			}
		});
		System.out.println(Thread.currentThread());
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
		ExecutorService executors = Executors.newFixedThreadPool(THREAD_NUM);
		long current = System.currentTimeMillis();
		try {
			int num = 0;
			for (int i = 0; i < MSG_SIZE; i++) {
				quotationInfo = createQuotationInfo();
				record = new ProducerRecord<String, String>(TOPIC, null, current,
						quotationInfo.getStockCode(), quotationInfo.toString());
				executors.submit(new KafkaProducerThread(producer, record));
			}

		} catch (Exception e) {
			LOG.error("Send message occurs exception", e);
		} finally {
			producer.close();
			executors.shutdown();
		}

	}
}
