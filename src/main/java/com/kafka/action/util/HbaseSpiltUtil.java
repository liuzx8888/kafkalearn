package com.kafka.action.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseSpiltUtil {



	private static int numberOfRregion = 1;
	private static int serversNum = 4;
	private static int deadserversNum = 0;
	private static float memstoreUppLimit = (float) 0.4;
	private static float memstoreFlush = 134217728;

	static byte[] newBound = null;
	static byte[] byterowkey = null;
	static byte[] newbyterowkey = null;



	public static int calchash(byte a[], int offset, int length) {
		if (a == null)
			return 0;
		int result = 1;
		for (int i = offset; i < offset + length; i++) {
			result = 31 * result + a[i];
		}
		return result;
	}

	public static byte rowkey_hash(byte a[], int offset, int length, int regionNum) {

		int calchash = calchash(a, offset, length);
		byte rerutnHash = (byte) Math.abs(calchash % regionNum);
		return rerutnHash;

	}

	public static byte[] new_rowkey_hash(byte rowsalt, String rowkey) {
		newBound = byterowkey = newbyterowkey = null;
		newBound = HbaseSaltSpilt.fillKey(new byte[] { rowsalt }, 5);
		byterowkey = Bytes.toBytes(rowkey);
		newbyterowkey = new byte[newBound.length + byterowkey.length];
		System.arraycopy(newBound, 0, newbyterowkey, 0, newBound.length);
		System.arraycopy(byterowkey, 0, newbyterowkey, newBound.length, byterowkey.length);
		return newbyterowkey;

	}

	/* 
	 * 5. 一个表“加多少盐合适”？
			当可用block cache的大小小于表数据大小时，较优的slated bucket是和region server数量相同，这样可以得到更好的读写性能。
			当表的数量很大时，基本上会忽略blcok cache的优化收益，大部分数据仍然需要走磁盘IO。比如对于10个region server集群的大表，
			可以考虑设计64~128个slat buckets。
	 * 表预分区的合理个数: 如果 <=block cache 范围内的表 只需要跟 RegionServer 的数量一样多就可以
	 * 				      如果  >=block cache 范围内的表 且表的数据量在中等  设置为64
	 *				      如果  >=block cache 范围内的表 且表的数据量在超大  设置为128 
	 */
	
	public int tableSpiltNum(int tableSizeDescripe) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		serversNum = admin.getClusterStatus().getServersSize();
		deadserversNum = admin.getClusterStatus().getDeadServers();
		memstoreUppLimit = Float.valueOf(conf.get("hbase.regionserver.global.memstore.upperLimit", "0.4"));
		memstoreFlush = Float.valueOf(conf.get("hbase.hregion.memstore.flush.size", "134217728")) / 1024 / 1024;

        int spiltNum = 0;

        if(tableSizeDescripe == 1 ) {
        	spiltNum=serversNum; 
        }
        
        if(tableSizeDescripe == 2 ) {
        	spiltNum=64; 
        }   
  
        if(tableSizeDescripe == 3 ) {
        	spiltNum=128; 
        }       
		return spiltNum;
		
	}

}
