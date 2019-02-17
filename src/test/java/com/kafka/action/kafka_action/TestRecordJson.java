package com.kafka.action.kafka_action;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.util.AvroSchemaUtil;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.ConvertDateType;
import com.kafka.action.util.SystemEnum;

public class TestRecordJson extends HashMap<String, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5627262577630813713L;

	public static void main(String[] args) throws IOException {

		// String str = "{\r\n" + " \"table\": \"DBO.TAB\", \r\n" + " \"op_type\":
		// \"I\", \r\n"
		// + " \"op_ts\": \"2019-01-21 14:05:18.446967\", \r\n"
		// + " \"current_ts\": \"2019-01-21T22:26:10.481001\", \r\n"
		// + " \"pos\": \"00000000400000050285\", \r\n" + " \"primary_keys\": [\r\n" + "
		// \"ID\"\r\n"
		// + " ], \r\n" + " \"after\": {\r\n" + " \"ID\": 211170, \r\n"
		// + " \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + " \"AGE\": 99,
		// \r\n"
		// + " \"NAME\": \"kkrrr\"\r\n" + " }\r\n" + "}\r\n";
		String str = "{\"table\":\"DBO.TAB\",\"op_type\":\"I\",\"op_ts\":\"2019-01-23 06:00:27.945417\",\"current_ts\":\"2019-01-27T15:10:49.945417\",\"pos\":\"00000000420000115169\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":220637,\"BIRTHDATE\":\"2019-01-25 20:02:59.945417\",\"AGE\":99,\"NAME\":\"kkyyy\"}}\r\n"
				+ "";

		String tableschema = AvroSchemaUtil.getSchemaStr(str);
		System.out.println(tableschema);
		Schema schema = new Schema.Parser().parse(tableschema);
		GenericRecord table = new GenericData.Record(schema);
		Map<String, Object> mapafter = AvroSchemaUtil.mapafter;
		Map<String, Object> mapbefore = AvroSchemaUtil.mapbefore;

		for (int i = 0; i < 100; i++) {
			if (mapafter.size() > 0) {
				for (Entry<String, Object> entry : mapafter.entrySet()) {
					//table.put(entry.getKey(), entry.getValue());
					table.put(entry.getKey(), String.valueOf(i));
					System.out.println(table);
				}
			}

			if (mapbefore.size() > 0) {
				for (Entry<String, Object> entry : mapbefore.entrySet()) {
					table.put(entry.getKey(), entry.getValue());
				}
			}

		}

		System.out.println(table.toString());

		File localfile = new File("D:\\test.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter).setCodec(CodecFactory.snappyCodec());
		DataFileWriter<GenericRecord> dataFileWriter = null;
		dataFileWriter = writer.create(schema, localfile);
		dataFileWriter.append(table);
		writer.close();
		dataFileWriter.close();

		// FileSystem fs = null;
		// Configuration conf =
		// ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
		// fs = FileSystem.get(conf);
		// Path path = new Path("/OGG/TAB/TAB_1.avro");
		// if (!fs.exists(path)) {
		// fs.createNewFile(path);
		// }
		// FSDataOutputStream outputStream = fs.append(path);
		//
		// DatumWriter<GenericRecord> datumWriter = new
		// GenericDatumWriter<GenericRecord>(schema);
		// DataFileWriter<GenericRecord> writer = new
		// DataFileWriter(datumWriter).setCodec(CodecFactory.snappyCodec());
		//
		//
		//
		// DataFileWriter<GenericRecord> dataFileWriter = null;
		//// dataFileWriter = writer.create(schema, outputStream);
		//// dataFileWriter.append(table);
		//// writer.close();
		//// dataFileWriter.close();
		//// outputStream.close();
		//
		// dataFileWriter=writer.appendTo(new FsInput(path, conf), outputStream);
		// dataFileWriter.append(table);
		// writer.close();
		// dataFileWriter.close();
		// outputStream.close();

	}

}
