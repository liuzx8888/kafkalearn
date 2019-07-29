package com.kafka.action.util;

public class LongUtil {

	final static long[] sizeTable = { 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, 9999999999L,
			99999999999L, 999999999999L, 9999999999999L, 99999999999999L, 999999999999999L, 9999999999999999L,
			99999999999999999L, 999999999999999999L, Long.MAX_VALUE };

	public static int stringSize(long x) {
		for (int i = 0;; i++)
			if (x <= sizeTable[i])
				return i + 1;
	}
	
	
}
