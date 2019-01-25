package com.kafka.action.kafka_action;

import java.lang.reflect.Type;

import com.alibaba.fastjson.parser.deserializer.ExtraProcessor;
import com.alibaba.fastjson.parser.deserializer.ExtraTypeProvider;

public class MyExtraProcessor implements ExtraProcessor, ExtraTypeProvider {

	@Override
	public Type getExtraType(Object object, String key) {
		// TODO Auto-generated method stub
		VO vo = (VO) object;
		if ("value".equals(key)) {
			return int.class;
		}
		return null;
	}

	@Override
	public void processExtra(Object object, String key, Object value) {
		// TODO Auto-generated method stub
		VO vo = (VO) object;
		vo.getAttributes().put(key, value);
	}

}
