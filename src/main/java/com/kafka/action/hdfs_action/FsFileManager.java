package com.kafka.action.hdfs_action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class FsFileManager {

	public static final Logger LOG = Logger.getLogger(FsSystem.class);
	public static FileSystem FS = null;
	public static Configuration CONF = null;

	static {

		Properties prop = ConfigUtil.getProperties(SystemEnum.HDFS);
		CONF = ConfigUtil.getConfiguration(prop);
		try {
			FS = FileSystem.get(CONF);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public List<FileStatus> getFile(Path path) throws IOException {
		FileStatus[] files = FS.listStatus(path);
		return Arrays.asList(files);
	}

	public int File_Id(Path path, float size) throws FileNotFoundException, IOException {
		int begin_id = 1;

		FileStatus[] files = FS.listStatus(path);
		List<FileStatus> FileStatus = Arrays.asList(files);
		for (FileStatus File : FileStatus) {
			if ((File.getLen() + size) > 134217728)
				begin_id++;
		}

		return begin_id;

	}

	public Path getpath(String topic, float size) throws FileNotFoundException, IllegalArgumentException, IOException {

		String path = "/OGG/" + topic.replace("OGG_", "");
		String sub_topic = topic.replace("OGG_", "");
		String path_new = "/" + "OGG" + "/" + sub_topic;

		if (!FS.exists(new Path(path_new + "/" + sub_topic + "_1.pquet"))) {
			FS.createNewFile(new Path(path_new + "/" + sub_topic + "_1.pquet"));
		}

		int id = File_Id(new Path(path), size);
		String path_Id = path_new + "/" + sub_topic + "_" + String.valueOf(id) + ".pquet";
		Path pathId = new Path(path_Id);
		return pathId;

	}
}
