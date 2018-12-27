package com.kafka.action.chapter6.avro;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.log4j.Logger;

import kafka.common.TopicAndPartition;
import kafka.consumer.AssignmentContext;
import kafka.consumer.ConsumerThreadId;
import kafka.consumer.PartitionAssignor;
import kafka.utils.Pool;

public class StockPartitionor implements Partitioner, PartitionAssignor {

	private static final Logger LOG = Logger.getLogger(StockPartitionor.class);
	/* 分区数 */
	private static final Integer PARTITIONS = 6;

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public Pool<String, scala.collection.mutable.Map<TopicAndPartition, ConsumerThreadId>> assign(
			AssignmentContext arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		if (key == null) {
			return 0;
		}
		String stockCode = String.valueOf(key);
		try {
			int partitionId = Integer.valueOf(stockCode.substring(stockCode.length() - 2)) % PARTITIONS;
			return partitionId;
		} catch (NumberFormatException e) {
			// TODO: handle exception
			LOG.error("股票代码分区有问题：" + stockCode, e);
			return 0;
		}

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
