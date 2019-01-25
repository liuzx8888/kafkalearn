package com.kafka.action.util;

import java.util.HashMap;
import java.util.Map;

public  class ConvertDateType {
	// String("string"), Integer("int"), Short("short"), Byte("byte"), Long("long"),
	// Float("float"), Double(
	// "double"), Boolean("boolean");
	private static Map<String, String> datatype = new HashMap<String, String>();
	static {

		datatype.put("String", "string");
		datatype.put("Integer", "int");
		datatype.put("Short", "short");
		datatype.put("Byte", "byte");
		datatype.put("Long", "long");
		datatype.put("Float", "float");
		datatype.put("Double", "double");
		datatype.put("Boolean", "boolean");
	}

	public static String returnDatetype(String datetype) {
		return datatype.get(datetype);
	}

}
