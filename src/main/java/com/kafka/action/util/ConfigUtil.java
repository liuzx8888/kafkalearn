package com.kafka.action.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;

public class ConfigUtil {

	public final static Logger LOG = Logger.getLogger(ConfigUtil.class);
	public static Properties prop = null;
	public static Configuration conf = null;

	@Test
	public static Properties getProperties(SystemEnum system) {
		InputStream in = null;
		in = ConfigUtil.class.getClassLoader().getSystemResourceAsStream(system.getSysname());
		prop = new Properties();
		try {
			prop.load(in);
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
		return prop;

	}

	public static Configuration getConfiguration(Properties prop) {

		conf = new Configuration();
		for (Entry<Object, Object> entry : prop.entrySet()) {
			conf.set(entry.getKey().toString(), entry.getValue().toString());
		}
		return conf;

	}
}
