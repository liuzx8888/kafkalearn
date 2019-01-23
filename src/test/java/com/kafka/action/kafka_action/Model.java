package com.kafka.action.kafka_action;

import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

public class Model {

	public int id;

	@JSONField(alternateNames= {"user","person"})
	public String name;

	public String demo;

	@Override
	public String toString() {
		return "Model [id=" + id + ", name=" + name + ", demo=" + demo + "]";
	}
	

	

}
