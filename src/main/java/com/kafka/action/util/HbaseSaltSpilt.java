package com.kafka.action.util;

import java.nio.ByteBuffer;

public class HbaseSaltSpilt {
	public static int saltBucketNum = 1;

	public int getSaltBucketNum() {
		return saltBucketNum;
	}

	public HbaseSaltSpilt(int saltBucketNum) {
		this.saltBucketNum = saltBucketNum;
	}

	public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
		byte[][] splits = new byte[saltBucketNum - 1][];
		for (int i = 1; i < saltBucketNum; i++) {
			splits[i - 1] = new byte[] { (byte) i };
		}
		return splits;
	}

	public static byte[] fillKey(byte[] key, int length) {
		if (key.length > length) {
			throw new IllegalStateException();
		}
		if (key.length == length) {
			return key;
		}
		byte[] newBound = new byte[length];
		System.arraycopy(key, 0, newBound, 0, key.length);
		return newBound;
	}

	public byte[][] splitRegionKey() {
		System.out.println("saltBucketNum:" + saltBucketNum);
		byte[][] splitkey = new byte[saltBucketNum - 1][];
		byte[][] key_SplitPoints = this.getSalteByteSplitPoints(saltBucketNum);
		for (int i = 1; i < saltBucketNum; i++) {
			byte[] key = key_SplitPoints[i - 1];
			int length = 5;
			byte[] newBound = this.fillKey(key, length);
			splitkey[i - 1] = newBound;
		}
		return splitkey;
	}

	public static byte[] newRowKey(byte[] oldrowkey, byte rowsalt) {
		byte[] newBound = HbaseSaltSpilt.fillKey(new byte[] { rowsalt }, 5);
		byte[] byterowkey = oldrowkey;
		byte[] newbyterowkey = new byte[newBound.length + byterowkey.length];
		System.arraycopy(newBound, 0, newbyterowkey, 0, newBound.length);
		System.arraycopy(byterowkey, 0, newbyterowkey, newBound.length, byterowkey.length);
		return newbyterowkey;
	}

	public static <T> T originalRowkey (byte[] rowkey,T datetype) {
		byte[] originalRowkey;
		T returnKey = null;
		
		if( datetype instanceof String ) {
			byte[] newrowkey = new byte[rowkey.length-5];
			System.arraycopy(rowkey, 5, newrowkey, 0, rowkey.length-5);
			originalRowkey=newrowkey; 
			 Class<T> Clazz =(Class<T>) String.class;
			 returnKey = Clazz.cast(originalRowkey);
		}
		if(datetype instanceof Long) {
			byte[] newrowkey = new byte[rowkey.length-5];
			System.arraycopy(rowkey, 5, newrowkey, 0, rowkey.length-5);
		 	ByteBuffer buffer = ByteBuffer.allocate(rowkey.length+5); 
			buffer.put(rowkey, 0, rowkey.length);
            buffer.flip();
            Class<T> Clazz =(Class<T>) Long.class;
			returnKey =Clazz.cast(buffer.getLong());
		}		
		
		   return returnKey;
		
	}
	
}
