package com.kafka.action.hdfs_action;

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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.util.ConvertDateType;

public class AvroToHdfs {

	public static void avroSchema(Path path,ConsumerRecord<String, String> msg,FSDataOutputStream outputStream) throws IOException {

		String msg_value = msg.value();
		Object json = JSONArray.parse(msg_value);
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
			String[] pkarray = p.matcher(primary_keystr).replaceAll("").split(",");
			Boolean flag = false;
			for (String primary_key : pkarray) {
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

		Schema schema = new Schema.Parser().parse(tableschema.toString());
		GenericRecord table = new GenericData.Record(schema);

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter).setCodec(CodecFactory.snappyCodec());
		writer.create(schema, outputStream);
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
		writer.append(table);
		writer.close();	
	}

}
