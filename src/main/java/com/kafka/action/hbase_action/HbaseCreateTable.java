package com.kafka.action.hbase_action;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import com.kafka.action.util.ConntionUtil;

public class HbaseCreateTable {
	Configuration conf = ConntionUtil.getConf();
	Connection conn = ConntionUtil.getConn();

	public HTableDescriptor HTableDescriptor(String tablename, String familyName, String Coprocessorpath) {
		TableName tablename_ = TableName.valueOf(tablename);
		HTableDescriptor htd = new HTableDescriptor(tablename_);
		HColumnDescriptor hcd;

		if (familyName == null) {
			hcd = new HColumnDescriptor(HbaseMetaData.default_family);
		} else {
			hcd = new HColumnDescriptor(familyName);
		}
		htd.addFamily(hcd);
		if (Coprocessorpath != null) {
			htd.setValue("COPROCESSOR$1", Coprocessorpath + "|" + SecondIndexObserver.class.getCanonicalName() + "|"
					+ Coprocessor.PRIORITY_USER);
		}
		return htd;
	}

	public boolean createTable(String tablename, HTableDescriptor htd, byte[][] splitKeys) throws IOException {
		boolean rs = false;
		TableName tablename_ = TableName.valueOf(tablename);
		Admin admin = conn.getAdmin();
		if (admin.tableExists(tablename_)) {
			admin.createTable(htd, splitKeys);

			rs = true;
		}
		return rs;

	}

}
