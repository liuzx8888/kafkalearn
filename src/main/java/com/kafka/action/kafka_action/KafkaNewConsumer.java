package com.kafka.action.kafka_action;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

public class KafkaNewConsumer implements Consumer {

	public static final Logger LOG = Logger.getLogger(KafkaProducerThread.class);
	public static final int MSG_SIZE = 100;
	public static final int TIME_OUT = 100;
	/* public static final String TOPIC = "stock-quotation"; */
	public static final String TOPIC = "TAB";
	public static final String GROUPID = "test";
	public static final String CLIENTID = "test";
	/*
	 * private static final String BROKER_LIST =
	 * "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";
	 */
	public static final String BROKER_LIST = "192.168.1.70:9092,192.168.1.71:9092,192.168.1.72:9092,192.168.1.73:9092";
	public static final int AUTOCOMMITOFFSET = 0;

	public static Properties pops = null;

	public static KafkaConsumer<String, String> kafkaConsumerconsumer = null;

	static {
		// 1.构建用于实例化KafkaConsumer 的 properties 信息
		Properties pops = initProperties();
		// 2.初始化一个KafkaProducer
		kafkaConsumerconsumer = new KafkaConsumer<>(pops);
	}

	/* 初始化配置文件 */
	@SuppressWarnings("unused")
	public static Properties initProperties() {

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
	public static void subscribeTopicAuto(KafkaConsumer<String, String> consumer, String topic) {
		if (AUTOCOMMITOFFSET == 1) {
			consumer.subscribe(Arrays.asList(topic));
			ConsumerTopicMessage(consumer);
		} else {
			LOG.info("设置了手动提交，请检查配置信息！！！");
		}

	}

	@SuppressWarnings("unused")
	public static void subscribeTopicCustom1(KafkaConsumer<String, String> consumer, String topic) {
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

	/* 订阅主题, 消费消息,手动提交偏移量 */
	public InputStream subscribeTopicCustom(KafkaConsumer<String, String> consumer, String topic) {
		int minCommitSize = 10;// 至少需要处理10条再提交
		int icount = 0;// 消息计数器
		int icount1 = 0;// 消息计数器
		InputStream in = null;
		List<String> msgs = null;
		if (AUTOCOMMITOFFSET == 0) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				try {
					msgs = new LinkedList<String>();
					ConsumerRecords<String, String> records = consumer.poll(TIME_OUT);
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("消费的消息: partition = %d,offset = %d, key = %s ,value = %s%n",
								record.partition(), record.offset(), record.key(), record.value());
						msgs.add(record.value());
						icount++;
						icount1++;
					}

					if (icount >= minCommitSize) {
						System.out.println(icount1);
						System.out.println(msgs);
						in = new BufferedInputStream(new ByteArrayInputStream(msgs.toString().getBytes()));
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

						icount = 0;
						return in;
					}

				} catch (Exception e) {
					// TODO: handle exception
					LOG.error("消费消息发生异常！！", e);
					break;
				}
				/*
				 * finally { consumer.close(); }
				 */
			}
		}
		return in;
	}

	/* 订阅特定分区 */
	@SuppressWarnings("unused")
	public static void subscribeTopicPartition(KafkaConsumer<String, String> consumer, String topic,
			int... partitions) {
		ArrayList<TopicPartition> Topicpartitions = new ArrayList<TopicPartition>();
		for (int partitionId : partitions) {
			Topicpartitions.add(new TopicPartition(topic, partitionId));
		}

		consumer.assign(Topicpartitions);
		ConsumerTopicMessage(consumer);

	}

	/* 订阅主题, 消费消息,按时间戳消费消息 */
	@SuppressWarnings("unused")
	public static void subscribeTopicTimestamp(KafkaConsumer<String, String> consumer, String topic,
			int... partitions) {
		ArrayList<TopicPartition> Topicpartitions = new ArrayList<TopicPartition>();
		for (int partitionId : partitions) {
			/* Topicpartitions.add(new TopicPartition(topic, partitionId)); */
			consumer.assign(Arrays.asList(new TopicPartition(topic, partitionId)));

			try {
				Map<TopicPartition, Long> timestampToSearch = new HashMap<TopicPartition, Long>();
				TopicPartition partition = new TopicPartition(TOPIC, partitionId);

				// 查询12小时之前的
				timestampToSearch.put(partition, (System.currentTimeMillis() - 72 * 360000 * 1000));
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
	public static void ConsumerTopicMessage(KafkaConsumer<String, String> consumer) {
		try {
			ConsumerRecords<String, String> records = consumer.poll(10000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("消费的消息: %n  partition = %d,offset = %d, key = %s , value = %s%n", record.partition(),
						record.offset(), record.key(), record.value());
			}

		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("消费消息发生异常！！", e);
		}

		/*
		 * finally { consumer.close(); }
		 */
	}

	@SuppressWarnings("unused")
	public String MsgsToHdfs(KafkaConsumer<String, String> consumer, String topic) throws IOException {

		InputStream inputStream = null;
		FSDataOutputStream outputStream = null;
		Configuration conf = null;
		FileSystem fs = null;
		String Time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String path = "/" + topic + "/" + topic;
		Path ph = new Path(path);
		try {
			conf = new Configuration();
			conf.set("mapreduce.jobtracker.address", "192.168.1.70:49001");
			conf.set("mapreduce.framework.name", "yarn");
			conf.set("yarn.resourcemanager.address", "192.168.1.70:8032");
			conf.setBoolean("dfs.support.append", true);
			conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
			conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		int minCommitSize = 10;// 至少需要处理10条再提交
		int icount = 0;// 消息计数器
		int icount1 = 0;// 消息计数器
		List<String> msgs = null;

		if (AUTOCOMMITOFFSET == 0) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				try {
					if (fs == null) {
						fs = FileSystem.get(conf);
					}
					msgs = new LinkedList<String>();
					ConsumerRecords<String, String> records = consumer.poll(1);
					for (ConsumerRecord<String, String> record : records) {
						System.out.printf("消费的消息: partition = %d,offset = %d, key = %s ,value = %s%n",
								record.partition(), record.offset(), record.key(), record.value());
						msgs.add(record.value());
						icount++;
						icount1++;
					}

					if (icount >= minCommitSize) {
						System.out.println(icount1);
						System.out.println(msgs.toString());

						if (!fs.exists(ph)) {
							fs.createNewFile(ph);
						}
						outputStream = fs.append(ph);
						outputStream.write(msgs.toString().toString().getBytes("UTF-8"));
						outputStream.write("/n".getBytes("UTF-8"));
						fs.close();
						/*
						 * inputStream = new BufferedInputStream(new
						 * ByteArrayInputStream(msgs.toString().getBytes()));
						 * System.out.println(inputStream.toString().length());
						 * 
						 * try { while (inputStream.read() != -1) { IOUtils.copyBytes(inputStream,
						 * outputStream, 4096000, false); return "写入HDFS成功"; } } catch (Exception e) {
						 * // TODO: handle exception return "写入HDFS失败"; }
						 */

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

						icount = 0;

					}
				} catch (Exception e) {
					// TODO: handle exception
					LOG.error("消费消息发生异常！！", e);
					// break;
				}
			}
		}
		return null;
	}

	public static void main(String[] args) throws IOException {
		/*
		 * for (int i = 0; i < 6; i++) { KafkaNewConsumer target = new
		 * KafkaNewConsumer(); target.subscribeTopicAuto(consumer, TOPIC); new
		 * Thread(target).start(); }
		 */

		KafkaNewConsumer target = new KafkaNewConsumer();
		/* target.subscribeTopicAuto(kafkaConsumerconsumer, TOPIC); */
		/* target.subscribeTopicCustom(kafkaConsumerconsumer, TOPIC); */
		/* target.subscribeTopicTimestamp(kafkaConsumerconsumer,TOPIC,0); */
		String rs = target.MsgsToHdfs(kafkaConsumerconsumer, TOPIC);
		System.out.println(rs);
		kafkaConsumerconsumer.close();

	}
}
