package com.kafka.action.kafka_action;

import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

public class DFWAppendTest {
	  public static class Sample {
		    CharSequence foo;

		    public Sample(CharSequence bar) {
		      this.foo = bar;
		    }
		  }

		  public static void main(String[] args) throws Exception {
		    Configuration conf = ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
		    FileSystem fs = FileSystem.get(conf);
		    
		    
		    
		    
		    Schema sample = ReflectData.get().getSchema(Sample.class);
		    
		    ReflectDatumWriter<Sample> rdw = new ReflectDatumWriter<DFWAppendTest.Sample>(
		        Sample.class);
		    
		    DataFileWriter<Sample> dfwo = new DataFileWriter<DFWAppendTest.Sample>(rdw);
		    
		    Path filePath = new Path("/sample.avro");
		    OutputStream out = fs.create(filePath);
		    DataFileWriter<Sample> dfw = dfwo.create(sample, out);
		    dfw.append(new Sample("Eggs2"));
		    dfw.append(new Sample("Spam2"));
		    dfw.close();
		    out.close();
		    OutputStream aout = fs.append(filePath);
		    dfw = dfwo.appendTo(new FsInput(filePath, conf), aout);
		    dfw.append(new Sample("Monty2"));
		    dfw.append(new Sample("Python2"));
		    dfwo.close();
		    aout.close();
		  }
}
