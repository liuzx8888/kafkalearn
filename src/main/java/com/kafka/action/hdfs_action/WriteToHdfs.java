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

	public static Boolean writeData(Path path, List<ConsumerRecord<String, String>> msgs, int init_createFile) {
		Boolean rs = false;
		int msgs_count = 0;
		try {
			if (msgs.size() > 0) {
				FileSystem fs = null;
				Configuration conf = ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
				fs = FileSystem.get(conf);
				if (fileDocFormat.equals("avro")) {
					for (ConsumerRecord<String, String> msg : msgs) {
						int avro_1_1 = ((msgs_count + init_createFile) == 0 ? init_createFile : msgs_count+100);
						AvroToHdfs.avroSchema(path, msg, fs, avro_1_1);
						msgs_count++;
					}
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
