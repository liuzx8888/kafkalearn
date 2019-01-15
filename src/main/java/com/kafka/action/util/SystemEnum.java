package com.kafka.action.util;

public enum SystemEnum {
	KAFKA("Kafka_Config.properties"), HDFS("Hdfs_Config.properties");

	private String sysname;

	private SystemEnum(String sysname) {
		this.sysname = sysname;

	}

	public String getSysname() {
		return sysname;
	}

	public void setSysname(String sysname) {
		this.sysname = sysname;
	}

}
