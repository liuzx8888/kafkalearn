package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSON;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class WriteToHdfs {
	private static Properties HDFSPROP = null;
	private static String fileDocFormat = null;
	static {
		HDFSPROP = ConfigUtil.getProperties(SystemEnum.HDFS);
		fileDocFormat = HDFSPROP.getProperty("fileDocFormat");
	}

	public static Boolean writeData(Path path, List<ConsumerRecord<String, String>> msgs) {
		Boolean rs = false;
		try {
			if (msgs.size() > 0) {
				FSDataOutputStream outputStream = null;
				FileSystem fs = null;
				Configuration conf = ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
				fs = FileSystem.get(conf);
				outputStream = fs.append(path);
				if (fileDocFormat.equals("avro")) {
					 for(ConsumerRecord<String, String> msg :msgs) {
						 AvroToHdfs.avroSchema(path,msg ,outputStream);
					 }
				}
				fs.close();
				outputStream.close();
				rs = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		return rs;

	}

}
