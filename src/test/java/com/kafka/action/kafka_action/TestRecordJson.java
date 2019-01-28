package com.kafka.action.kafka_action;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.kafka_action.DFWAppendTest.Sample;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.ConvertDateType;
import com.kafka.action.util.SystemEnum;

public class TestRecordJson {

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
		String str = "{\"table\":\"DBO.TAB\",\"op_type\":\"I\",\"op_ts\":\"2019-01-23 06:00:27.945417\",\"current_ts\":\"2019-01-27T15:10:49.532001\",\"pos\":\"00000000420000115169\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":220637,\"BIRTHDATE\":\"2019-01-25 20:02:59.390000000\",\"AGE\":99,\"NAME\":\"kkyyy\"}}\r\n"
				+ "";
		System.out.println(str);
		Object json = JSONArray.parse(str);
		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		String tab = ((String) JSONPath.eval(json, "$.table")).replace("DBO.", "");
		Object primary_keys = (Object) JSONPath.eval(json, "$.primary_keys");
		String op_type = (String) JSONPath.eval(json, "$.op_type");
		Object before = (Object) JSONPath.eval(json, "$.before");
		Object after = (Object) JSONPath.eval(json, "$.after");
		Map<String, Object> mapafter = new HashMap<String, Object>();
		Map<String, Object> mapbefore = new HashMap<String, Object>();
		if (after != null && after != "") {
			mapafter = JSONObject.parseObject(after.toString(), Feature.OrderedField);
		}
		if (before != null && before != "") {
			mapbefore = JSONObject.parseObject(before.toString(), Feature.OrderedField);
		}

		mapafter.put("LASTUPDATEDTTM", current_ts);
		if (op_type.equals("I")) {
			mapafter.put("ISDELETED", 0);
		}

		if (op_type.equals("U")) {
			mapafter.put("ISDELETED", 0);
			Object pk_before = null;
			Object pk_after = null;
			String primary_keystr = primary_keys.toString();
			Pattern p = Pattern.compile("\\[|\\]|\"");
			String[] pkarr = p.matcher(primary_keystr).replaceAll("").split(",");
			Boolean flag = false;
			for (String primary_key : pkarr) {
				pk_before = (Object) JSONPath.eval(before, "$." + primary_key + "");
				pk_after = (Object) JSONPath.eval(after, "$." + primary_key + "");
				if (pk_before.equals(pk_after)) {
					flag = true;
				} else {
					flag = false;
					break;
				}

			}

			if (!flag) {
				mapbefore.put("ISDELETED", 1);
			}

		}

		if (op_type.equals("D")) {
			mapafter.put("ISDELETED", 1);
		}

		System.out.println(JSON.toJSONString(mapafter));

		StringBuilder tableschema = new StringBuilder();
		tableschema = tableschema.append("{\"namespace\": \"com.kafka.action.chapter6.avro\",\r\n"
				+ "\"type\": \"record\",\r\n" + " \"name\": \"" + tab + "\"," + "\n" + " \"fields\": [" + "\n");

		for (Entry<String, Object> entry : mapafter.entrySet()) {
			tableschema
					.append(" {\"name\": \"" + entry.getKey() + "\",\"type\": \""
							+ ConvertDateType
									.returnDatetype(entry.getValue().getClass().getTypeName().replace("java.lang.", ""))
							+ "\"}," + "\n");
		}
		tableschema.deleteCharAt(tableschema.length() - 2);
		tableschema.append(" ]\r\n" + "}\r\n" + "");
		System.out.println(tableschema.toString());

		Schema schema = new Schema.Parser().parse(tableschema.toString());
		GenericRecord table = new GenericData.Record(schema);

	

		if (mapafter.size() > 0) {
			for (Entry<String, Object> entry : mapafter.entrySet()) {
				table.put(entry.getKey(), entry.getValue());
			}
		}

		if (mapbefore.size() > 0) {
			for (Entry<String, Object> entry : mapbefore.entrySet()) {
				table.put(entry.getKey(), entry.getValue());
			}
		}

		FileSystem fs = null;
		Configuration conf = ConfigUtil.getConfiguration(ConfigUtil.getProperties(SystemEnum.HDFS));
		fs = FileSystem.get(conf);
		Path path = new Path("/OGG/TAB/TAB_1.avro");
		if (!fs.exists(path)) {
			fs.createNewFile(path);
		}
		FSDataOutputStream outputStream = fs.append(path);

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter).setCodec(CodecFactory.snappyCodec());		

	
	
		DataFileWriter<GenericRecord> dataFileWriter = null;
//		dataFileWriter = writer.create(schema, outputStream);
//		dataFileWriter.append(table);
//		writer.close();
//		dataFileWriter.close();
//		outputStream.close();
		
		dataFileWriter=writer.appendTo(new FsInput(path, conf), outputStream);
		dataFileWriter.append(table);
		writer.close();
		dataFileWriter.close();
		outputStream.close();		

	}

}
