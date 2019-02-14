package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.util.ConvertDateType;

public class ParquetToHdfs {
	public static void parquetSchema(Path path, ConsumerRecord<String, String> msg, FileSystem fs, int init_createFile)
			throws IOException {

		String msg_value = msg.value();
		Object json = JSONArray.parse(msg_value);
		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		String tab = ((String) JSONPath.eval(json, "$.table")).replace("DBO.", "");
		Object primary_keys = (Object) JSONPath.eval(json, "$.primary_keys");
		String op_type = (String) JSONPath.eval(json, "$.op_type");
		Object before = (Object) JSONPath.eval(json, "$.before");
		Object after = (Object) JSONPath.eval(json, "$.after");
		Map<String, Object> mapafter = new LinkedHashMap<String, Object>();
		Map<String, Object> mapbefore = new LinkedHashMap<String, Object>();
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
//		message schema {
//			  optional int64 log_id;
//			  optional binary idc_id;
//			  optional int64 house_id;
//			  optional int64 src_ip_long;
//			  optional int64 dest_ip_long;
//			  optional int64 src_port;
//			  optional int64 dest_port;
//			  optional int32 protocol_type;
//			  optional binary url64;
//			  optional binary access_time;
//			}
	
		StringBuilder tableschema = new StringBuilder();
		tableschema = tableschema.append("message schema {");
		for (Entry<String, Object> entry : mapafter.entrySet()) {
			tableschema.append("optional"
					+" "
					+ ConvertDateType
							.returnParDatetype(entry.getValue().getClass().getTypeName().replace("java.lang.", ""))
					+ " " + entry.getKey() + ";" );
		}
		tableschema.append("}");
		System.out.println(tableschema.toString());

		MessageType schema = MessageTypeParser.parseMessageType(tableschema.toString());
		ExampleParquetWriter.Builder builder =null;
		if (init_createFile == 0) {
			 builder = ExampleParquetWriter.builder(path)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
					.withCompressionCodec(CompressionCodecName.SNAPPY)
					.withConf(fs.getConf())
					.withType(schema);
		} else {
			 builder = ExampleParquetWriter.builder(path)
					.withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
					.withCompressionCodec(CompressionCodecName.SNAPPY)
					.withConf(fs.getConf())
					.withType(schema);
		}
		

		/**
		 * Hadoop 写入信息
		 */

		ParquetWriter<Group> writer = builder.build();
		SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
		Group table = groupFactory.newGroup();
		if (mapafter.size() > 0) {
			for (Entry<String, Object> entry : mapafter.entrySet()) {
				table.append(entry.getKey(), (String) entry.getValue());
			}
		}

		if (mapbefore.size() > 0) {
			for (Entry<String, Object> entry : mapbefore.entrySet()) {
				table.append(entry.getKey(), (String) entry.getValue());
			}
		}

		writer.write(table);
		writer.close();
	}
}
