package com.kafka.action.kafka_action;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;

public class TestRecordJson {

	public static void main(String[] args) {

		String str1 = "{\r\n" + "    \"table\": \"DBO.TAB\", \r\n" + "    \"op_type\": \"I\", \r\n"
				+ "    \"op_ts\": \"2019-01-21 14:05:18.446967\", \r\n"
				+ "    \"current_ts\": \"2019-01-21T22:26:10.481001\", \r\n"
				+ "    \"pos\": \"00000000400000050285\", \r\n" + "    \"primary_keys\": [\r\n" + "        \"ID\"\r\n"
				+ "    ], \r\n" + "    \"after\": {\r\n" + "        \"ID\": 211170, \r\n"
				+ "        \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + "        \"AGE\": 99, \r\n"
				+ "        \"NAME\": \"kkrrr\"\r\n" + "    }\r\n" + "}\r\n";

		String str2 = "{\r\n" + "    \"ID\": 211170, \r\n"
				+ "    \"BIRTHDATE\": \"2019-01-22 00:58:17.903000000\", \r\n" + "    \"AGE\": 99, \r\n"
				+ "    \"NAME\": \"kkrrr\"\r\n" + "}";

		Object json = JSON.parseObject(str1.toString());

		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		String tab = ((String) JSONPath.eval(json, "$.table")).replace("DBO.", "");;
		Object primary_keys = (Object) JSONPath.eval(json, "$.primary_keys");
		String op_type = (String) JSONPath.eval(json, "$.op_type");
		Object before = (Object) JSONPath.eval(json, "$.before");
		Object after = (Object) JSONPath.eval(json, "$.after");

		Object json1 = JSON.parseObject(after.toString());

		Map<String, Object> map = JSONObject.parseObject(json1.toString());
		map.put("LASTUPDATEDTTM", current_ts);

		if (op_type.equals("I")) {
			map.put("ISDELETED", "0");
		}

		if (op_type.equals("D")) {
			map.put("ISDELETED", "1");
		}

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder = stringBuilder.append("\"namespace\": \"com.kafka.action.chapter6.avro\",\r\n"
				+ "    \"type\": \"record\",\r\n" + "    \"name\": \"AvroStockQuotation\","+ "\n"+ " \"fields\": ["+ "\n");

		for (Entry<String, Object> entry : map.entrySet()) {

			stringBuilder.append("{\"name\": \"" + entry.getKey() + "\",\"type\":  \""
					+ entry.getValue().getClass().getName() + "\"}," + "\n");
		}
		System.out.println(stringBuilder.toString());

		System.out.println(current_ts.getClass());
	}

}
