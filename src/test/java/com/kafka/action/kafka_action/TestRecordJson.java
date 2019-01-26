package com.kafka.action.kafka_action;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.internal.JacksonUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.util.ConvertDateType;

public class TestRecordJson {

	public static void main(String[] args) throws IOException {

//		String str = "{\r\n" + "    \"table\": \"DBO.TAB\", \r\n" + "    \"op_type\": \"I\", \r\n"
//				+ "    \"op_ts\": \"2019-01-21 14:05:18.446967\", \r\n"
//				+ "    \"current_ts\": \"2019-01-21T22:26:10.481001\", \r\n"
//				+ "    \"pos\": \"00000000400000050285\", \r\n" + "    \"primary_keys\": [\r\n" + "        \"ID\"\r\n"
//				+ "    ], \r\n" + "    \"after\": {\r\n" + "        \"ID\": 211170, \r\n"
//				+ "        \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + "        \"AGE\": 99, \r\n"
//				+ "        \"NAME\": \"kkrrr\"\r\n" + "    }\r\n" + "}\r\n";
		String str="{\r\n" + 
				"    \"table\": \"DBO.TAB\", \r\n" + 
				"    \"op_type\": \"U\", \r\n" + 
				"    \"op_ts\": \"2019-01-21 14:05:18.446967\", \r\n" + 
				"    \"current_ts\": \"2019-01-21T22:26:10.481001\", \r\n" + 
				"    \"pos\": \"00000000400000050285\", \r\n" + 
				"    \"primary_keys\": [\r\n" + 
				"        \"ID\"\r\n" + 
				"    ], \r\n" + 
				"    \"before\": {\r\n" + 
				"        \"ID\": 211171, \r\n" + 
				"        \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + 
				"        \"AGE\": 99, \r\n" + 
				"        \"NAME\": \"kkrrr\"\r\n" + 
				"    }, \r\n" + 
				"    \"after\": {\r\n" + 
				"        \"ID\": 211170, \r\n" + 
				"        \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + 
				"        \"AGE\": 99, \r\n" + 
				"        \"NAME\": \"kkrrr\"\r\n" + 
				"    }\r\n" + 
				"}";
		System.out.println(str);
		Object json = JSONArray.parse(str);
		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		String tab = ((String) JSONPath.eval(json, "$.table")).replace("DBO.", "");
		Object primary_keys = (Object) JSONPath.eval(json, "$.primary_keys");
		String op_type = (String) JSONPath.eval(json, "$.op_type");
		Object before = (Object) JSONPath.eval(json, "$.before");
		Object after = (Object) JSONPath.eval(json, "$.after");
		Map<String, Object> mapafter = null;
		Map<String, Object> mapbefore = null;
		if (after != null) {
			mapafter = JSONObject.parseObject(after.toString(), Feature.OrderedField);
		}
		if (before != null) {
			mapbefore = JSONObject.parseObject(before.toString(), Feature.OrderedField);
		}

		mapafter.put("LASTUPDATEDTTM", current_ts);

		if (op_type.equals("I")) {
			mapafter.put("ISDELETED", 0);
		}

		if (op_type.equals("U")) {
			String pk_before = (String) JSONPath.eval(before, "\"$." + primary_keys.toString().replace("\"", "").replace("[", "").replace("]", "")+"\"");
			String pk_after = (String) JSONPath.eval(after, "\"$." + primary_keys.toString().replace("\"", "").replace("[", "").replace("]", "") + "\"");
			
			if (pk_before.equals(pk_after)) {
				mapafter.put("ISDELETED", 0);
			} else {
				mapafter.putAll(mapbefore);
				mapafter.put("ISDELETED", 1);
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

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter).setCodec(CodecFactory.snappyCodec());

		File avro = new File("D:\\Test.Avro");
		writer.create(schema, avro);

		for (Entry<String, Object> entry : mapafter.entrySet()) {
			table.put(entry.getKey(), entry.getValue());
		}

		writer.append(table);
		writer.close();
	}

}
