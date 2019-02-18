package com.kafka.action.hdfs_action;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;

import com.kafka.action.util.AvroSchemaUtil;

public class ParquetToHdfs {
	private static final Logger LOG = Logger.getLogger(ParquetToHdfs.class);

	public static void parquetSchema(String topic, List<ConsumerRecord<String, String>> msgs, FileSystem fs)
			throws IOException {

		int msgs_count = 0;

		FsFileManager FsFile = new FsFileManager();
		Path path = FsFile.getpath(topic, 1);
		int init_createFile = FsFile.init_createFile;
		if (fs.exists(path)) {
			fs.delete(path);
		}
		String tableschema = AvroSchemaUtil.getSchema(msgs.get(0));
		Schema schema = new Schema.Parser().parse(tableschema);
		AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);

		for (ConsumerRecord<String, String> msg : msgs) {
			AvroSchemaUtil.getSchema(msg);
			Map<String, Object> mapafter = AvroSchemaUtil.mapafter;
			Map<String, Object> mapbefore = AvroSchemaUtil.mapbefore;

			/**
			 * Hdfs 写入信息
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
			writer.write(record);
			msgs_count++;
		}

		writer.close();
		LOG.info("msgs_count:"+msgs_count);

		// MessageType schemaPar = new AvroSchemaConverter().convert(schema);
		// ExampleParquetWriter.Builder builder = null;
		// if (init_createFile == 0) {
		// builder =
		// ExampleParquetWriter.builder(path).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
		// .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
		// .withCompressionCodec(CompressionCodecName.SNAPPY).withConf(fs.getConf()).withType(schemaPar);
		// } else {
		// builder =
		// ExampleParquetWriter.builder(path).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
		// .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
		// .withCompressionCodec(CompressionCodecName.SNAPPY).withConf(fs.getConf()).withType(schemaPar);
		// }
		//
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
		//
		// writer.write(table);
		// writer.close();

	}

}
