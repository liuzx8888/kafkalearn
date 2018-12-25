package com.kafka.action.kafka_action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class KafkaNewConsumer extends Thread {

	private static final Logger LOG = Logger.getLogger(KafkaProducerThread.class);
	private static final int MSG_SIZE = 100;
	private static final int TIME_OUT = 100;
	private static final String TOPIC = "stock-quotation";
	private static final String GROUPID = "test";
	private static final String CLIENTID = "test";
	private static final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";
	private static final int AUTOCOMMITOFFSET = 1;

	private static Properties pops = null;

	private static KafkaConsumer<String, String> consumer = null;

	static {
		// 1.构建用于实例化KafkaConsumer 的 properties 信息
		Properties pops = initProperties();
		// 2.初始化一个KafkaProducer
		consumer = new KafkaConsumer<>(pops);
	}

	/* 初始化配置文件 */
	@SuppressWarnings("unused")
	private static Properties initProperties() {

		pops = new Properties();
		pops.put("bootstrap.servers", BROKER_LIST);
		pops.put("group.id", GROUPID);
		pops.put("client.id", CLIENTID);
		if (AUTOCOMMITOFFSET == 0) {
			pops.put("fetch.max.bytes", 1024);// 一次获取最大数据了为1K
			pops.put("enable.auto.commit", false); // 消费者偏移量管理，关闭自动提交
		}
		if (AUTOCOMMITOFFSET == 1) {
			pops.put("enable.auto.commit", true);// 消费者偏移量管理，设置自动提交
			pops.put("auto.commit.interval.ms", 1000); // 消费者偏移量管理，设置自动提交间隔
		}

		pops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		pops.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		return pops;
	}

	/* 订阅主题, 消费消息,自动提交偏移量 */
	@SuppressWarnings("unused")
	private static void subscribeTopicAuto(KafkaConsumer<String, String> consumer, String topic) {
		if (AUTOCOMMITOFFSET == 1) {
			consumer.subscribe(Arrays.asList(topic));
			ConsumerTopicMessage(consumer);
		} else {
			LOG.info("设置了手动提交，请检查配置信息！！！");
		}
	}

	/* 订阅主题, 消费消息,手动提交偏移量 */
	@SuppressWarnings("unused")
	private static void subscribeTopicCustom(KafkaConsumer<String, String> consumer, String topic) {
		if (AUTOCOMMITOFFSET == 0) {
			consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					// TODO Auto-generated method stub
					consumer.commitAsync(); // 提交偏移量

				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					// TODO Auto-generated method stub
					long committedOffset = -1;
					for (TopicPartition topicPartition : partitions) {
						// 获取该分区的 偏移量
						committedOffset = consumer.committed(topicPartition).offset();
						// 重置偏移量到上次提交的偏移量下一个位置处开始消费
						consumer.seek(topicPartition, committedOffset);

					}
				}
			});
		} else {
			LOG.info("设置了自动提交，请检查配置信息！！！");
		}

		ConsumerTopicMessage(consumer);

	}

	@SuppressWarnings("unused")
	private static void subscribeTopicCustom1(KafkaConsumer<String, String> consumer, String topic) {
		int minCommitSize = 10;// 至少需要处理10条再提交
		int icount = 0;// 消息计数器

		if (AUTOCOMMITOFFSET == 0) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				try {
					ConsumerRecords<String, String> records = consumer.poll(TIME_OUT);
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("partition = %d,offset = %d, key = %d ,value = %s%d", record.partition(),
								record.offset(), record.key(), record.value());
						icount++;
					}
					if (icount >= minCommitSize) {
						consumer.commitAsync(new OffsetCommitCallback() {

							@Override
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
									Exception exception) {
								// TODO Auto-generated method stub
								if (exception == null) {
									LOG.info("提交成功!!!");
								} else {
									LOG.error("提交失败!!!");
								}
							}
						});
					}
					icount++;
				} catch (Exception e) {
					// TODO: handle exception
					LOG.error("消费消息发生异常！！", e);
				} finally {
					consumer.close();
				}
			}
		}
	}

	/* 订阅特定分区 */
	@SuppressWarnings("unused")
	private static void subscribeTopicPartition(KafkaConsumer<String, String> consumer, String topic,
			int... partitions) {
		ArrayList<TopicPartition> Topicpartitions = new ArrayList<TopicPartition>();
		for (int partitionId : partitions) {
			Topicpartitions.add(new TopicPartition(topic, partitionId));
		}

		consumer.assign(Topicpartitions);
		ConsumerTopicMessage(consumer);

	}

	/* 订阅主题, 消费消息,按时间戳消费消息 */
	private static void subscribeTopicTimestamp(KafkaConsumer<String, String> consumer, String topic,
			int... partitions) {
		ArrayList<TopicPartition> Topicpartitions = new ArrayList<TopicPartition>();
		for (int partitionId : partitions) {
			/* Topicpartitions.add(new TopicPartition(topic, partitionId)); */
			consumer.assign(Arrays.asList(new TopicPartition(topic, partitionId)));

			try {
				Map<TopicPartition, Long> timestampToSearch = new HashMap<TopicPartition, Long>();
				TopicPartition partition = new TopicPartition(TOPIC, partitionId);

				// 查询12小时之前的
				timestampToSearch.put(partition, (System.currentTimeMillis() - 12 * 3600 * 1000));
				// 会返回时间大于等于查找时间的第一个偏移量
				Map<TopicPartition, OffsetAndTimestamp> offSet = consumer.offsetsForTimes(timestampToSearch);
				OffsetAndTimestamp offsetAndTimestamp = null;

				for (Entry<TopicPartition, OffsetAndTimestamp> entry : offSet.entrySet()) {
					offsetAndTimestamp = entry.getValue();
					if (offsetAndTimestamp != null) {
						// 重置消费起始偏移量
						consumer.seek(partition, entry.getValue().offset());
					}
				}
				ConsumerTopicMessage(consumer);

			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}

	/* 获取消息 */
	private static void ConsumerTopicMessage(KafkaConsumer<String, String> consumer) {
		try {
			ConsumerRecords<String, String> records = consumer.poll(TIME_OUT);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("partition = %d,offset = %d, key = %d ,value = %s%d", record.partition(),
						record.offset(), record.key(), record.value());

			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("消费消息发生异常！！", e);
		} finally {
			consumer.close();
		}
	}

	public static void main(String[] args) {
		for (int i = 0; i < 6; i++) {
			KafkaNewConsumer target = new KafkaNewConsumer();
			target.subscribeTopicAuto(consumer, TOPIC);
			new Thread(target).start();
		}

	}
}
