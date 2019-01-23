package com.kafka.action.kafka_action;

import java.io.IOException;
import java.lang.reflect.Type;

import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;

public class UserSerialize implements ObjectSerializer {

	@Override
	public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features)
			throws IOException {
		// TODO Auto-generated method stub

		String memo = String.valueOf(object.hashCode());
		serializer.write(memo);
	}

}
