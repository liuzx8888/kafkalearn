package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class FsSystem {

	private static final Logger LOG = Logger.getLogger(FsSystem.class);
	private static FileSystem FS = null;
	private static Configuration CONF = null;
	private static FSDataInputStream inputStream = null;

	static {

		try {
			CONF = new Configuration();
			FS = FileSystem.get(CONF);
			CONF.set("mapreduce.jobtracker.address", "192.168.1.70:49001");
			CONF.set("mapreduce.framework.name", "yarn");
			CONF.set("yarn.resourcemanager.address", "192.168.1.70:8032");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Boolean mkdirs(FileSystem fs, String path) throws Exception {

		if (fs.exists(new Path(path))) {

			System.err.println(path + " 已经存在, 请重新创建！！！");
			return false;
		} else {
			fs.mkdirs(fs, new Path(path), new FsPermission(FsPermission.getDirDefault()));
			return true;
		}

	}

	private String outputdata(FileSystem fs, InputStream inputStream, String path)
			throws IllegalArgumentException, IOException {

		FSDataOutputStream outputStream = fs.create(new Path(path), new Progressable() {
			@Override
			public void progress() {
				// TODO Auto-generated method stub

			}
		});
		IOUtils.copyBytes(inputStream, outputStream, 4096, false);
		return null;

	}

}
