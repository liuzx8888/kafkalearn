package com.kafka.action.kafka_action;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.security.JaasUtils;

import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class TopicManager {
	private static  String ZK_CONNECT ;
	private static  int SESSION_TIMEOUT ;
	private static  int CONNECT_TIMEOUT ;
	private static ZkUtils utils;
	private static Properties prop =null;
	static {
		Properties prop = ConfigUtil.getProperties(SystemEnum.ZOOKEEPER);
		ZK_CONNECT= prop.getProperty("ZK_CONNECT");
		SESSION_TIMEOUT= Integer.parseInt(prop.getProperty("SESSION_TIMEOUT"));
		CONNECT_TIMEOUT= Integer.parseInt(prop.getProperty("CONNECT_TIMEOUT"));		
		utils = null;
		utils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
	}

	/*
	 * 创建主题
	 */
	public static void createtopic(ZkUtils utils, String Topic, int partitions, int replicationFactor,
			Properties Properties) {
		AdminUtils.createTopic(utils, Topic, partitions, replicationFactor, Properties,
				AdminUtils.createTopic$default$6());
	}

	/*
	 * 查询主题
	 */

	public static List<String> getTopicList(String prefix, String postfix) {
		List<String> allTopicList = JavaConversions.seqAsJavaList(utils.getAllTopics());
		List<String> topicList = allTopicList.stream()
				.filter(topic -> topic.startsWith(prefix) && topic.endsWith(postfix)).collect(Collectors.toList());

		return topicList;
	}

	/*
	 * 删除主题
	 */
	public static void deletetopic(ZkUtils utils, String Topic) {
		AdminUtils.deleteTopic(utils, Topic);

	}

	/*
	 * 查询主题属性
	 */

	public static void getproperties(ZkUtils utils, String Topic) {
		Properties props = AdminUtils.fetchEntityConfig(utils, ConfigType.Topic(), Topic);
		Iterator it = props.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Object, Object> entry = (Entry<Object, Object>) it.next();
			Object key = entry.getKey();
			Object value = entry.getValue();
			System.out.println(key + "    " + value);
		}
	}

	/*
	 * 修改主题属性
	 */

	public static void alterproperties(ZkUtils utils, String Topic, String propkey, String propvalue) {
		Properties props = AdminUtils.fetchEntityConfig(utils, ConfigType.Topic(), Topic);
		props.setProperty(propkey, propvalue);
		AdminUtils.changeTopicConfig(utils, Topic, props);

		/*
		 * Iterator it = props.entrySet().iterator(); while (it.hasNext()) { Map.Entry
		 * entry = (Entry) it.next(); Object key = entry.getKey(); Object value =
		 * entry.getValue(); System.out.println(key + "    " + value); }
		 */
	}

	/*
	 * 增加分区
	 */
	public static void addpartition(ZkUtils utils, String Topic, int numPartitions) {
		AdminUtils.addPartitions(utils, Topic, numPartitions, "", true, AdminUtils.addPartitions$default$6());
	}

	/*
	 * 分区副本重分配
	 */

	public static void assignreplicas(ZkUtils utils, String Topic, int numPartitions, int replications) {
		/* 获取代理元数据 */
		Seq<BrokerMetadata> getBrokerMetadatas = AdminUtils.getBrokerMetadatas(utils,
				AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());

		/* 生成副本分区方案 */
		scala.collection.Map<Object, Seq<Object>> replicaAssign = (scala.collection.Map<Object, Seq<Object>>) AdminUtils
				.assignReplicasToBrokers(getBrokerMetadatas, numPartitions, replications,
						AdminUtils.assignReplicasToBrokers$default$4(), AdminUtils.assignReplicasToBrokers$default$5());

		/* 修改重分区方案 */

		AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(utils, Topic, replicaAssign, new Properties(), true);

	}

//	public static void main(String[] args) {
//		String Topic = "stock-quotation";
//		/* TopicManager.createtopic(utils, Topic, 4, 2, new Properties()); */
//
//		// TopicManager.alterproperties(utils, Topic, "max.message.bytes", "404800");
//		// TopicManager.getproperties(utils, Topic);
//		/* TopicManager.addpartition(utils,Topic, 9); */
//		/* TopicManager.assignreplicas(utils, Topic, 11, 4); */
//		List<String> allTopicList = TopicManager.getTopicList("OGG", "");
//		for (String topic : allTopicList) {
//			System.out.println(topic);
//		}
//
//		utils.close();
//
//	}
}
