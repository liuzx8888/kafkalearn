package com.kafka.action.kafka_action;

import java.util.HashMap;
import java.util.Map;

public class VO {
	private int id;
	private Map<String, Object> attributes = new HashMap<String, Object>();

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Map<String, Object> getAttributes() {
		return attributes;
	}
}
