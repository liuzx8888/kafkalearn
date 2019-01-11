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

	public static final Logger LOG = Logger.getLogger(FsSystem.class);
	public static FileSystem FS = null;
	public static Configuration CONF = null;
	public static FSDataInputStream inputStream = null;

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

	public Boolean mkdirs(FileSystem fs, String path) throws Exception {
		Boolean rs = null;
		if (fs.exists(new Path(path))) {

			System.out.println(path + " 已经存在, 请重新创建！！！");
			rs = false;
		} else {
			fs.mkdirs(fs, new Path(path), new FsPermission(FsPermission.getDirDefault()));
			System.out.println(path + " 已经成功创建！！！");
			rs = true;
		}

		return rs;

	}

	public String outputdata(FileSystem fs, InputStream inputStream, String path)
			throws IllegalArgumentException, IOException {

		FSDataOutputStream outputStream = fs.create(new Path(path), new Progressable() {
			@Override
			public void progress() {
				// TODO Auto-generated method stub

			}
		});


		while (inputStream.read() != -1) {
			IOUtils.copyBytes(inputStream, outputStream, 4096000, false);
		}
		/* IOUtils.closeStream(outputStream); */
		return null;

	}

	/*
	 * public static void main(String[] args) { FsSystem fs = new FsSystem(); try {
	 * fs.mkdirs(FS, "/TAB"); } catch (Exception e) { // TODO Auto-generated catch
	 * block e.printStackTrace(); } }
	 */

}
