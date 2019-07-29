package com.kafka.action.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ConntionUtil {
	public static Configuration conf = null;
	public static Connection conn = null;
	
	
	static {
		conf = HBaseConfiguration.create();
		try {
		conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Configuration getConf() {
		return conf;
	}

	public static Connection getConn() {
		return conn;
	}



	
	

}
