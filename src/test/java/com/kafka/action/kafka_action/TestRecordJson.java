package com.kafka.action.kafka_action;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.kafka.action.util.ConvertDateType;

public class TestRecordJson {

	public static void main(String[] args) throws IOException {

		String str1 = "{\r\n" + "    \"table\": \"DBO.TAB\", \r\n" + "    \"op_type\": \"I\", \r\n"
				+ "    \"op_ts\": \"2019-01-21 14:05:18.446967\", \r\n"
				+ "    \"current_ts\": \"2019-01-21T22:26:10.481001\", \r\n"
				+ "    \"pos\": \"00000000400000050285\", \r\n" + "    \"primary_keys\": [\r\n" + "        \"ID\"\r\n"
				+ "    ], \r\n" + "    \"after\": {\r\n" + "        \"ID\": 211170, \r\n"
				+ "        \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + "        \"AGE\": 99, \r\n"
				+ "        \"NAME\": \"kkrrr\"\r\n" + "    }\r\n" + "}\r\n";

		Object json = JSON.parseObject(str1.toString());

		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		String tab = ((String) JSONPath.eval(json, "$.table")).replace("DBO.", "");
		Object primary_keys = (Object) JSONPath.eval(json, "$.primary_keys");
		String op_type = (String) JSONPath.eval(json, "$.op_type");
		Object before = (Object) JSONPath.eval(json, "$.before");
		Object after = (Object) JSONPath.eval(json, "$.after");

		Object jsonafter = JSON.parseObject(after.toString());

		Map<String, Object> map = JSONObject.parseObject(jsonafter.toString());
		map.put("LASTUPDATEDTTM", current_ts);

		if (op_type.equals("I")) {
			map.put("ISDELETED", 0);
		}

		if (op_type.equals("D")) {
			map.put("ISDELETED", 1);
		}

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder = stringBuilder
				.append("{\"namespace\": \"com.kafka.action.chapter6.avro\",\r\n" + "    \"type\": \"record\",\r\n"
						+ "    \"name\": \"" + tab + "\"," + "\n" + "    \"fields\": [" + "\n");

		for (Entry<String, Object> entry : map.entrySet()) {
			stringBuilder
					.append("                {\"name\": \"" + entry.getKey() + "\",\"type\":  \""
							+ ConvertDateType
									.returnDatetype(entry.getValue().getClass().getTypeName().replace("java.lang.", ""))
							+ "\"}," + "\n");
		}
		stringBuilder.deleteCharAt(stringBuilder.length() - 2);
		stringBuilder.append("              ]\r\n" + "}\r\n" + "");
		System.out.println(stringBuilder.toString());

		Schema schema = new Schema.Parser().parse(stringBuilder.toString());
		GenericRecord user1 = new GenericData.Record(schema);

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> writer = new DataFileWriter(datumWriter);

		File avro = new File("D:\\Test.Avro");
		writer.create(schema, avro);

		for (Entry<String, Object> entry : map.entrySet()) {
			user1.put(entry.getKey(), entry.getValue());
		}
		writer.append(user1);
		writer.close();
	}

}
