package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.avro.AvroParquetWriter;

import com.kafka.action.util.AvroSchemaUtil;

public class ParquetToHdfs {
	public static void parquetSchema(String topic, ConsumerRecord<String, String> msg, FileSystem fs)
			throws IOException {
		String tableschema = AvroSchemaUtil.getSchema(msg);
		Schema schema = new Schema.Parser().parse(tableschema);
		Map<String, Object> mapafter = AvroSchemaUtil.mapafter;
		Map<String, Object> mapbefore = AvroSchemaUtil.mapbefore;


		// message schema {
		// optional int64 log_id;
		// optional binary idc_id;
		// optional int64 house_id;
		// optional int64 src_ip_long;
		// optional int64 dest_ip_long;
		// optional int64 src_port;
		// optional int64 dest_port;
		// optional int32 protocol_type;
		// optional binary url64;
		// optional binary access_time;
		// }

		// MessageType schema =
		// MessageTypeParser.parseMessageType(tableschema.toString());
		// ExampleParquetWriter.Builder builder =null;
		// if (init_createFile == 0) {
		// builder = ExampleParquetWriter.builder(path)
		// .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
		// .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
		// .withCompressionCodec(CompressionCodecName.SNAPPY)
		// .withConf(fs.getConf())
		// .withType(schema);
		// } else {
		// builder = ExampleParquetWriter.builder(path)
		// .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
		// .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
		// .withCompressionCodec(CompressionCodecName.SNAPPY)
		// .withConf(fs.getConf())
		// .withType(schema);
		// }

		// ParquetWriter<Group> writer = builder.build();
		// SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
		// Group table = groupFactory.newGroup();
		// if (mapafter.size() > 0) {
		// for (Entry<String, Object> entry : mapafter.entrySet()) {
		// table.append(entry.getKey(), (String) entry.getValue());
		// }
		// }
		//
		// if (mapbefore.size() > 0) {
		// for (Entry<String, Object> entry : mapbefore.entrySet()) {
		// table.append(entry.getKey(), (String) entry.getValue());
		// }
		// }

		// writer.write(table);
		// writer.close();

		/**
		 * Hadoop 写入信息
		 */
		GenericRecord record = new GenericData.Record(schema);
		if (mapafter.size() > 0) {
			for (Entry<String, Object> entry : mapafter.entrySet()) {
				record.put(entry.getKey(), entry.getValue());
			}
		}
		
		if (mapbefore.size() > 0) {
			for (Entry<String, Object> entry : mapbefore.entrySet()) {
				record.put(entry.getKey(), entry.getValue());
			}
		}
		
		FsFileManager FsFile = new FsFileManager();
		Path path = FsFile.getpath(topic, record.toString().length());
		int init_createFile = FsFile.init_createFile;
		
		if (fs.exists(path)) {
			fs.delete(path);
		}
		AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);
		writer.write(record);
		writer.close();
	}
}
