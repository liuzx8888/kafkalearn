package com.kafka.action.kafka_action;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.kafka.action.chapter6.avro.TAB;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class HdfsReadAvro {
	
	public static void readFromAvro(InputStream is) throws IOException {
		DataFileStream<Object> reader = new DataFileStream<Object>(is, new GenericDatumReader<Object>());
		for (Object o : reader) {
			GenericRecord r = (GenericRecord) o;
			System.out.println(r.toString());
		}
		IOUtils.cleanup(null, is);
		IOUtils.cleanup(null, reader);
	}

	
	public static void deserializeAvroFromFile(String fileName) throws IOException {
	    File file = new File(fileName);
	    DatumReader<TAB> userDatumReader = new SpecificDatumReader<TAB>(TAB.class);
	    DataFileReader<TAB> dataFileReader = new DataFileReader<TAB>(file, userDatumReader);
	    TAB user = null;
	    System.out.println("----------------deserializeAvroFromFile-------------------");
	    while (dataFileReader.hasNext()) {
	        user = dataFileReader.next(user);
	        System.out.println(user);
	    }
	}	
	
	public static void main(String[] args) throws Exception {
//		Configuration config =  ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
//		FileSystem hdfs = FileSystem.get(config);
//		Path destFile = new Path(args[0]);
//		InputStream is = hdfs.open(destFile);
//		readFromAvro(is);
		HdfsReadAvro avro = new HdfsReadAvro();
		avro.deserializeAvroFromFile("D:\\TAB_1.avro");
		//avro.deserializeAvroFromFile("D:\\Test.Avro");
		//avro.deserializeAvroFromFile("D:\\sample.avro");

	}
	
}
