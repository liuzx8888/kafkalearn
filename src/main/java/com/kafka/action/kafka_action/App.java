package com.kafka.action.kafka_action;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.MAP;

import com.kafka.action.hdfs_action.FsFileManager;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class App {
	public static void main(String[] args) {
/*		System.out.println("Hello World!");*/
		final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";

/*		System.out.println(Arrays.asList(BROKER_LIST.split(",")));

		System.out.println(Arrays.asList(StringUtils.split(BROKER_LIST, ",")));

		System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));*/
		
			Properties prop= ConfigUtil.getProperties(SystemEnum.HDFS);
/*			System.out.println(prop);*/
			Configuration conf = new Configuration();
		    Iterator it  =  prop.entrySet().iterator();
		    while(it.hasNext()) {
		    	Map.Entry<String, String> entry  = (Entry<String, String>) it.next();
		    	System.out.printf(entry.getKey() + ":"+ entry.getValue() );
		    	System.out.println("");
		    	conf.set(entry.getKey(),entry.getValue());
		    }
		    
		    for (Entry<Object, Object> entry : prop.entrySet()) {
		    	System.out.printf(entry.getKey() + ":"+ entry.getValue() );
		    	System.out.println("");
			}
		    String topic="OGG_TAB";
		    String path = "/" + "OGG" + "/" + topic.substring(4, topic.length())+"/"+topic.substring(4, topic.length());
		    String path1 = "/" + "OGG" + "/" + topic.substring(4, topic.length());
                        
		    System.out.println(path1);
		    FsFileManager fileManager = new FsFileManager();
		    try {
		    	List<FileStatus> FileStatus=fileManager.getFile(new Path(path1));
		    	System.out.println(FileStatus.toString());
		    	int i =fileManager.File_Id(new Path(path1),522222222);
		    	System.out.println(i);
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            
/*				conf.set(properties.getKey().toString(),properties.getValue().toString());*/
			
		
		}
	}

