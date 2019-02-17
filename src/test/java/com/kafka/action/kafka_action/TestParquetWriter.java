/**
 * 创建日期：2017-8-2
 * 包路径：org.meter.parquet.TestParquetWriter.java
 * 创建者：meter
 * 描述：
 * 版权：copyright@2017 by meter !
 */
package com.kafka.action.kafka_action;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.Feature;
import com.kafka.action.util.ConfigUtil;
import com.kafka.action.util.SystemEnum;

/**
 * @author meter 文件名：TestParquetWriter @描述：
 */
public class TestParquetWriter {

	private static Logger logger = LoggerFactory.getLogger(TestParquetWriter.class);
	// private static String schemaStr = "message schema {" + "optional int64
	// log_id;" + "optional binary idc_id;"
	// + "optional int64 house_id;" + "optional int64 src_ip_long;" + "optional
	// int64 dest_ip_long;"
	// + "optional int64 src_port;" + "optional int64 dest_port;" + "optional int32
	// protocol_type;"
	// + "optional binary url64;" + "optional binary access_time;}";

	private static String schemaStr = "message schema {" + "optional INT64 ID;" + "optional BINARY BIRTHDATE;"
			+ "optional INT64 AGE;" + "optional BINARY NAME;" + "optional BINARY LASTUPDATEDTTM;"
			+ "optional INT64 ISDELETED;" + "}";

	static MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

	/**
	 * 创建时间：2017-8-3 创建者：meter 返回值类型：void
	 * 
	 * @描述：输出MessageType
	 */
	public static void testParseSchema() {
		logger.info(schema.toString());
	}

	// /**
	// * 创建时间：2017-8-3
	// * 创建者：meter
	// * 返回值类型：void
	// * @描述：获取parquet的Schema
	// * @throws Exception
	// */
	// public static void testGetSchema() throws Exception {
	// Configuration configuration = new Configuration();
	// // windows 下测试入库impala需要这个配置
	// System.setProperty("hadoop.home.dir",
	// "E:\\mvtech\\software\\hadoop-common-2.2.0-bin-master");
	// ParquetMetadata readFooter = null;
	// Path parquetFilePath = new
	// Path("file:///E:/mvtech/work/isms_develop/src/org/meter/parquet/2017-08-02-10_91014_DPI0801201708021031_470000.parq");
	// readFooter = ParquetFileReader.readFooter(configuration,
	// parquetFilePath, ParquetMetadataConverter.NO_FILTER);
	// MessageType schema =readFooter.getFileMetaData().getSchema();
	// logger.info(schema.toString());
	// }

	/**
	 * 创建时间：2017-8-3 创建者：meter 返回值类型：void
	 * 
	 * @描述：测试写parquet文件
	 * @throws IOException
	 */
	private static void testParquetWriter() throws IOException {
		testParseSchema();
		Path file = new Path("hdfs://192.168.1.70/output1");
		Properties prop = ConfigUtil.getProperties(SystemEnum.HDFS);

		FileSystem FS = FileSystem.get(ConfigUtil.getConfiguration(prop));
		ExampleParquetWriter.Builder builder = null;
		if (!FS.exists(file)) {
			builder = ExampleParquetWriter.builder(file).withWriteMode(ParquetFileWriter.Mode.CREATE)
					.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
					.withCompressionCodec(CompressionCodecName.SNAPPY)
					// .withConf(configuration)
					.withType(schema);
		} else {
			builder = ExampleParquetWriter.builder(file).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
					.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
					.withCompressionCodec(CompressionCodecName.SNAPPY)
					// .withConf(configuration)
					.withType(schema);
		}
		/*
		 * file, new GroupWriteSupport(), CompressionCodecName.SNAPPY, 256 * 1024 *
		 * 1024, 1 * 1024 * 1024, 512, true, false,
		 * ParquetProperties.WriterVersion.PARQUET_1_0, conf
		 */
		ParquetWriter<Group> writer = builder.build();
		SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
		// String[] access_log = { "111111", "22222", "33333", "44444", "55555",
		// "666666", "777777", "888888", "999999",
		// "101010" };
		String[] access_log = { "111", "1989-09-01", "22", "44444", "1989-09-01", "0", "777777", "888888", "999999",
				"101010" };
		for (int i = 0; i < 10000; i++) {
			// writer.write(groupFactory.newGroup().append("log_id",
			// Long.parseLong(access_log[0]))
			// .append("idc_id", access_log[1]).append("house_id",
			// Long.parseLong(access_log[2]))
			// .append("src_ip_long", Long.parseLong(access_log[3]))
			// .append("dest_ip_long", Long.parseLong(access_log[4]))
			// .append("src_port", Long.parseLong(access_log[5]))
			// .append("dest_port", Long.parseLong(access_log[6]))
			// .append("protocol_type", Integer.parseInt(access_log[7])).append("url64",
			// access_log[8])
			// .append("access_time", access_log[9]));
			writer.write(groupFactory.newGroup().append("ID", Long.parseLong(access_log[0]))
					.append("BIRTHDATE", access_log[1]).append("AGE", Long.parseLong(access_log[2]))
					.append("NAME", access_log[3]).append("LASTUPDATEDTTM", access_log[4])
					.append("ISDELETED", Long.parseLong(access_log[5])));
		}
		writer.close();
	}

	/**
	 * 创建时间：2017-8-3 创建者：meter 返回值类型：void
	 * 
	 * @throws IOException
	 * @描述：测试读parquet文件
	 */
	private static void testParquetReader() throws IOException {
		Path path = new Path("hdfs://192.168.1.70/OGG/TAB/TAB_1.parquet");
		ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), path);

		ParquetReader<Group> reader = builder.build();
		SimpleGroup group = (SimpleGroup) reader.read();
		logger.info("schema:" + group.getType().toString());
		logger.info("idc_id:" + group.getString(4, 0));

	}

	private static void testParquetWrite() throws IOException {
		String str = "{\"table\":\"DBO.TAB\",\"op_type\":\"U\",\"op_ts\":\"2019-01-23 06:00:27.945417\",\"current_ts\":\"2019-01-27T15:10:49.945417\",\"pos\":\"00000000420000115169\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":220637,\"BIRTHDATE\":\"2019-01-25 20:02:59.945417\",\"AGE\":99,\"NAME\":\"kkyyy\"}}\r\n";
		String str1 = "{\"namespace\": \"com.kafka.action.chapter6.avro\",\r\n" + " \"type\": \"record\",\r\n"
				+ " \"name\": \"TAB\",\r\n" + " \"fields\": [\r\n" + " {\"name\": \"ID\",\"type\": \"int\"},\r\n"
				+ " {\"name\": \"BIRTHDATE\",\"type\": \"string\"},\r\n" + " {\"name\": \"AGE\",\"type\": \"int\"},\r\n"
				+ " {\"name\": \"NAME\",\"type\": \"string\"},\r\n"
				+ " {\"name\": \"LASTUPDATEDTTM\",\"type\": \"string\"},\r\n"
				+ " {\"name\": \"ISDELETED\",\"type\": \"int\"}\r\n" + " ]\r\n" + "}";
		Schema schema = new Schema.Parser().parse(str1);
		Path path = new Path("hdfs://192.168.1.70/output1");
		AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);

		Object json = JSON.parse(str);
		Object after = (Object) JSONPath.eval(json, "$.after");
		String current_ts = ((String) JSONPath.eval(json, "$.current_ts")).replace("T", " ");
		Map<String, Object> mapafter = new LinkedHashMap<String, Object>();

		if (after != null && after != "") {
			mapafter = JSONObject.parseObject(after.toString(), Feature.OrderedField);
		}
		mapafter.put("LASTUPDATEDTTM", current_ts);
		mapafter.put("ISDELETED", 0);

		GenericRecord record = new GenericData.Record(schema);
		for (int i = 0; i < 100; i++) {
			for (Entry<String, Object> entry : mapafter.entrySet()) {
				//record.put(entry.getKey(), entry.getValue());
				record.put(entry.getKey(), String.valueOf(i));
			}
		}
        System.out.println("record"+record.toString());
		writer.write(record);
		writer.close();
	}

	/**
	 * 创建时间：2017-8-2 创建者：meter 返回值类型：void
	 * 
	 * @描述：
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// testGetSchema();
		// testParseSchema();
		// testParquetWriter();
		testParquetWrite();
		// testParquetReader();
	}

}
