package com.kafka.action.kafka_action;

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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import com.kafka.action.hdfs_action.FsFileManager;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class KafkaNewConsumer implements Consumer {

	private static final Logger LOG = Logger.getLogger(KafkaProducerThread.class);
	private static int TIME_OUT = 1000;
	/* public static final String TOPIC = "stock-quotation"; */

	private static int AUTOCOMMITOFFSET;
	private static Properties KAFKAPROP = null;
	private static Properties HDFSPROP = null;
	public static KafkaConsumer<String, String> kafkaConsumerconsumer = null;

	static {
		// 1.构建用于实例化KafkaConsumer 的 properties 信息

		KAFKAPROP = ConfigUtil.getProperties(SystemEnum.KAFKA);
		HDFSPROP = ConfigUtil.getProperties(SystemEnum.HDFS);

		if (KAFKAPROP.getProperty("enable.auto.commit").equalsIgnoreCase("true")) {
			AUTOCOMMITOFFSET = 1;
		} else {
			AUTOCOMMITOFFSET = 0;
		}

		TIME_OUT = Integer.parseInt(KAFKAPROP.getProperty("time_out"));
		KAFKAPROP.remove("TIME_OUT");
		// 2.初始化一个KafkaProducer
		kafkaConsumerconsumer = new KafkaConsumer<>(KAFKAPROP);
	}

	/* 订阅主题, 消费消息,自动提交偏移量 */
	@SuppressWarnings("unused")
	public static void subscribeTopicAuto(KafkaConsumer<String, String> consumer, String topic) {
		if (AUTOCOMMITOFFSET == 1) {
			consumer.subscribe(Arrays.asList(topic));
			ConsumerTopicMessage(consumer, topic);

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

		ConsumerTopicMessage(consumer, topic);

	}

	/* 订阅主题, 消费消息,手动提交偏移量 */
	@SuppressWarnings("unused")
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
		ConsumerTopicMessage(consumer, topic);

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
				TopicPartition partition = new TopicPartition(topic, partitionId);

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
				ConsumerTopicMessage(consumer, topic);

			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}

	/* 获取消息 */
	public static void ConsumerTopicMessage(KafkaConsumer<String, String> consumer, String topic) {
		List<String> msgs = null;

		try {
			msgs = new LinkedList<String>();
			ConsumerRecords<String, String> records = consumer.poll(TIME_OUT);

			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("消费的消息: %n  partition = %d,offset = %d, key = %s , value = %s%n", record.partition(),
						record.offset(), record.key(), record.value());
				msgs.add(record.value());
			}
			FsFileManager FsFile = new FsFileManager();
			String path = "/" + "OGG" + "/" + topic.substring(4, topic.length());
			int id = FsFile.File_Id(new Path(path), msgs.size(), topic.substring(4, topic.length()));
			String path_Id = path + "/" + topic.substring(4, topic.length()) + "_" + String.valueOf(id);

			Path pathId = new Path(path);

			writeToHdfs(pathId, msgs);

		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("消费消息发生异常！！", e);
		}

	}

	@SuppressWarnings("unused")
	public String MsgsToHdfs(KafkaConsumer<String, String> consumer, String topic) throws IOException {

		InputStream inputStream = null;
		String Time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		String path = "/" + topic + "/" + topic;
		Path ph = new Path(path);
		String rs = null;

		int minCommitSize = 10;// 至少需要处理10条再提交
		int icount = 0;// 消息计数器
		int icount1 = 0;// 消息计数器
		List<String> msgs = null;

		if (AUTOCOMMITOFFSET == 0) {
			consumer.subscribe(Arrays.asList(topic));
			while (true) {
				try {
					msgs = new LinkedList<String>();
					ConsumerRecords<String, String> records = consumer.poll(1000);
					if (records.toString().length() > 0) {
						for (ConsumerRecord<String, String> record : records) {
							System.out.printf("订阅消息：  topic = %s, partition = %s, offset = %d,key = %s, value = %s\n",
									record.topic(), record.partition(), record.offset(), record.key(), record.value());
							msgs.add(record.value());
							icount++;
							icount1++;
						}

						if (icount >= minCommitSize) {

							/*
							 * inputStream = new BufferedInputStream(new
							 * ByteArrayInputStream(msgs.toString().getBytes()));
							 * System.out.println(inputStream.toString().length());
							 * 
							 * try { while (inputStream.read() != -1) { IOUtils.copyBytes(inputStream,
							 * outputStream, 4096000, false); return "写入HDFS成功"; } } catch (Exception e) {
							 * // TODO: handle exception return "写入HDFS失败"; }
							 */

							if (writeToHdfs(ph, msgs)) {
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
								rs = "写入Hdfs" + ph.getName() + "成功！！！";
								;
								icount = 0;
							}
						}
					}
				} catch (Exception e) {
					// TODO: handle exception
					LOG.error("消费消息发生异常！！", e);
					rs = "写入HDFS失败！！";
					break;
				}
			}
		}
		System.out.println(rs);
		return rs;
	}

	public static Boolean writeToHdfs(Path path, List<String> msgs) throws IOException {
		Configuration conf = null;

		FSDataOutputStream outputStream = null;
		Boolean rs = null;

		conf = ConfigUtil.getConfiguration(HDFSPROP);
		FileSystem fs = FileSystem.get(conf);

		try {
			/*
			 * if (!fs.exists(path)) { fs.createNewFile(path); }
			 */
			outputStream = fs.append(path);
			if (msgs.size() > 0) {
				outputStream.write(msgs.toString().getBytes("utf-8"));
				outputStream.write("/n".getBytes("utf-8"));
				rs = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			rs = false;
			e.printStackTrace();
		} finally {
			fs.close();
			outputStream.close();
		}

		return rs;

	}

	public static void main(String[] args) throws IOException {
		/*
		 * for (int i = 0; i < 6; i++) { KafkaNewConsumer target = new
		 * KafkaNewConsumer(); target.subscribeTopicAuto(consumer, TOPIC); new
		 * Thread(target).start(); }
		 */

		List<String> topics = TopicManager.getTopicList("OGG", "");
		for (String topic : topics) {

			KafkaNewConsumer target = new KafkaNewConsumer();
			if (AUTOCOMMITOFFSET == 1) {
				target.subscribeTopicAuto(kafkaConsumerconsumer, topic);
			} else {
				String rs = target.MsgsToHdfs(kafkaConsumerconsumer, topic);
				System.out.println(rs);
			}
		}

		/* target.subscribeTopicCustom(kafkaConsumerconsumer, TOPIC); */
		/* target.subscribeTopicTimestamp(kafkaConsumerconsumer,TOPIC,0); */
		kafkaConsumerconsumer.close();

	}
}
