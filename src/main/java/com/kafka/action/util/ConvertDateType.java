package com.kafka.action.util;

import java.util.HashMap;
import java.util.Map;

public class ConvertDateType {
	// String("string"), Integer("int"), Short("short"), Byte("byte"), Long("long"),
	// Float("float"), Double(
	// "double"), Boolean("boolean");
	private static Map<String, String> Avrodatatype = new HashMap<String, String>();
	static {
		Avrodatatype.put("String", "string");
		Avrodatatype.put("Integer", "int");
		Avrodatatype.put("Short", "short");
		Avrodatatype.put("Byte", "byte");
		Avrodatatype.put("Long", "long");
		Avrodatatype.put("Float", "float");
		Avrodatatype.put("Double", "double");
		Avrodatatype.put("Boolean", "boolean");
		Avrodatatype.put("Char", "char");

	}
	private static Map<String, String> Parquetdatatype = new HashMap<String, String>();
	static {
		Parquetdatatype.put("String", "binary");
		Parquetdatatype.put("Integer", "int64");
		Parquetdatatype.put("Short", "binary");
		Parquetdatatype.put("Byte", "binary");
		Parquetdatatype.put("Long", "long");
		Parquetdatatype.put("Float", "float");
		Parquetdatatype.put("Double", "double");
		Parquetdatatype.put("Boolean", "boolean");
		Parquetdatatype.put("Char", "binary");

	}	
	public static String returnAvroDatetype(String datetype) {
		return Avrodatatype.get(datetype);
	}
	public static String returnParDatetype(String datetype) {
		return Parquetdatatype.get(datetype);
	}
}
