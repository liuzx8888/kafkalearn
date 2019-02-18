package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class WriteToHdfs {
	private static Properties HDFSPROP = null;
	private static String fileDocFormat = null;
	static {
		HDFSPROP = ConfigUtil.getProperties(SystemEnum.HDFS);
		fileDocFormat = HDFSPROP.getProperty("fileDocFormat");
	}

	public static Boolean writeData(String topic, List<ConsumerRecord<String, String>> msgs) {
		Boolean rs = false;
		int msgs_count = 0;

		try {
			if (msgs.size() > 0) {
				FileSystem fs = null;
				Configuration conf = ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
				fs = FileSystem.get(conf);
				if (fileDocFormat.equals("avro")) {
					AvroToHdfs.avroSchema(topic, msgs, fs);
				}
				if (fileDocFormat.equals("parquet")) {
					ParquetToHdfs.parquetSchema(topic, msgs, fs);
				}
				msgs_count = 0;
				fs.close();
				rs = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		return rs;

	}

}
