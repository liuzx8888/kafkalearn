package com.kafka.action.kafka_action;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.lf5.util.StreamUtils;
import org.jboss.netty.util.internal.StringUtil;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;


public class KafkaSimpleConsumer {
	private static final Logger LOG = Logger.getLogger(KafkaSimpleConsumer.class);
	/* 连接超时设置1分钟 */
	private static final int TIME_OUT = 60 * 1000;
	/* 设置读取消息的缓冲区大小 */
	private static final int BUFFER_SIZE = 1024 * 1024;
	/* 设置每次获取消息的条数 */
	private static final int FETCH_SIZE = 100000;
	/* broker 的端口 */
	private static final int PORT = 9092;
	/* 发生错误的最大重试次数 */
	private static final int MAX_ERROR_NUM = 3;

	private static final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";

	private static final String TOPIC = "stock-quotation-partition";
	/*
	 * 获取分区元数据信息
	 */
	private PartitionMetadata fetchPartitionMetadata(List<String> brokerid, int port, String topic, int partitionId) {
		SimpleConsumer consumer = null;
		TopicMetadataRequest metadataRequest = null;
		TopicMetadataResponse metadataResponse = null;
		List<TopicMetadata> topicMetadatas = null;
		try {
			for (String host : brokerid) {
				/* 1.构造一个消费者用于获取元数据执行者 */
				consumer = new SimpleConsumer(host, PORT, TIME_OUT, BUFFER_SIZE, "fetch-metadata");
				/* 2.构造请求主题的Request */
				metadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
				/* 3.发送获取主题元数据的请求 */
				try {
					metadataResponse = consumer.send(metadataRequest);
				} catch (Exception e) {
					// TODO: handle exception
					LOG.error("代理连接失败", e);
					continue;
				}
				/* 4.获取主题元数据列表 */
				topicMetadatas = metadataResponse.topicsMetadata();
				/* 5.主题列表中指定分区的元数据信息 */
				for (TopicMetadata topicMetadata : topicMetadatas) {
					for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
						if (partitionMetadata.partitionId() != partitionId) {
							continue;
						} else {
							return partitionMetadata;
						}

					}
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("获取分区元数据出错", e);
		} finally {
			if (consumer != null) {
				consumer.close();
			}
		}

		return null;

	}

	/*
	 * 获取消息偏移量信息
	 */
	@SuppressWarnings("unused")
	private long getLastOffSet(SimpleConsumer consumer, String topic, int partition, long begintime,
			String clientname) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestinfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		/* 设置获取消息的起始offset */
		requestinfo.put(topicAndPartition, new PartitionOffsetRequestInfo(begintime, 1));
		/* 构造获取 offset 请求 */
		OffsetRequest offsetRequest = new OffsetRequest(requestinfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientname);
		OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);

		if (offsetResponse.hasError()) {
			LOG.error("获取最后的偏移量出错" + offsetResponse.errorCode(topic, partition));
			return -1;
		}

		long[] offsets = offsetResponse.offsets(topic, partition);

		if (offsets == null || offsets.length == 0) {
			LOG.error("获取最后的偏移量出错,偏移量为空");
			return -1;
		}

		return offsets[0];

	}

	public void consume(List<String> brokerList, int port, String topic, int partitionId) {
		SimpleConsumer consumer = null;
		try {
			// 1.获取指定分区的元数据
			PartitionMetadata partitionMetadata = fetchPartitionMetadata(brokerList, port, topic, partitionId);
			if (partitionMetadata == null) {
				LOG.info(partitionId + "  元数据为空");
				return;
			}
			if (partitionMetadata.leader() == null) {
				LOG.info("找不到分区" + partitionId + "的 Leader！");
				return;
			}

			String leadBroker = partitionMetadata.leader().host();
			String clientId = "client-" + topic + partitionId;

			// 2.创建一个消费者，作为消费消息的真正的执行者
			consumer = new SimpleConsumer(leadBroker, port, TIME_OUT, BUFFER_SIZE, clientId);

			// 设置时间为 kafka.api.OffsetRequest.EarliestTime() 从最新的消息出开始
			long lastOffSet = getLastOffSet(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(),
					clientId);
			int errorNum = 0;
			FetchRequest fetchRequest = null;
			FetchResponse fetchResponse = null;
			while (lastOffSet > -1) {
				// 当在循环中出错，实例化consumer关闭并设置成null
				if (consumer == null) {
					consumer = new SimpleConsumer(leadBroker, port, TIME_OUT, BUFFER_SIZE, clientId);
				}
				// 3.构造获取消息的request
				fetchRequest = new FetchRequestBuilder().clientId(clientId)
						.addFetch(topic, partitionId, lastOffSet, FETCH_SIZE).build();

				// 4.获取响应并处理
				fetchResponse = consumer.fetch(fetchRequest);
				if (fetchResponse.hasError()) {
					errorNum++;
					if (errorNum > MAX_ERROR_NUM) {
						break;
					}
					// 获取错误码
					short errorCode = fetchResponse.errorCode(topic, partitionId);
					// offset 已经无效，因为在获取 lastOffSet 时设置为从最早开始时间，若是这种错误码,
					// 将时间设置为从LatestTime()开始查找
					if (ErrorMapping.OffsetOutOfRangeCode() == errorCode) {
						lastOffSet = getLastOffSet(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(),
								clientId);
						continue;
					} else if (ErrorMapping.OffsetsLoadInProgressCode() == errorCode) {
						Thread.sleep(3000);
						continue;
					} else {
						consumer.close();
						consumer = null;
						continue;
					}
				} else {
					errorNum = 0;
					long fetchNum = 0;
					for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
						long currentOffset = messageAndOffset.offset();
						if (currentOffset < lastOffSet) {
							LOG.error("获取旧偏移量" + currentOffset + "应该大于" + lastOffSet);
							continue;
						}
						lastOffSet = messageAndOffset.nextOffset();
						ByteBuffer playlode = messageAndOffset.message().payload();

						byte[] bytes = new byte[playlode.limit()];
						playlode.get(bytes);

						LOG.info("message" + (new String(bytes, "utf-8")) + ",offset:" + messageAndOffset.offset());
						fetchNum++;
					}
					if (fetchNum == 0) {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO: handle exception
							LOG.error("没有可取数据", e);
						}

					}

				}
			}

		} catch (InterruptedException | UnsupportedEncodingException e) {
			// TODO: handle exception
			LOG.error("消费数据发生异常！", e);
		} finally {
			if (consumer == null) {
				consumer.close();
			}

		}

	}

	public static void main(String[] args) {
		KafkaSimpleConsumer consumer = new KafkaSimpleConsumer();
/*		consumer.consume(Arrays.asList(BROKER_LIST.split(",")), PORT, TOPIC, 5);*/
		consumer.consume(Arrays.asList(StringUtils.split(BROKER_LIST, ",")), PORT, TOPIC, 0);
	}
}
