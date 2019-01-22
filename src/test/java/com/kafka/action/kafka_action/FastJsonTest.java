package com.kafka.action.kafka_action;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.config.types.Password;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.SerializationFeature;

public class FastJsonTest {

	// String json="{\"table\":\"DBO.TAB\",\"op_type\":\"I\",\"op_ts\":\"2019-01-21
	// 14:05:18.446967\",\"current_ts\":\"2019-01-21T22:26:10.481001\",\"pos\":\"00000000400000050285\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":211170,\"BIRTHDATE\":\"2019-01-22
	// 00:58:17.903000000\",\"AGE\":99,\"NAME\":\"kkrrr\"}" ;

	@Test
	public void ToJsonString() {

		Date date = new Date(System.currentTimeMillis());

		User user1 = new User(1, "test1", 18, new Password("112233"), date,"1111");
		User user2 = new User(2, "test2", 18, new Password("112233"), date,"2222");
		User user3 = new User(3, "test3", 18, new Password("112233"), date,"3333");
		List<User> users = new LinkedList<User>();
		users.add(user1);
		users.add(user2);
		users.add(user3);
		UserGroup group = new UserGroup(1, "test1", users);

		String userjson = JSON.toJSONString(user1);
		String usersjson = JSON.toJSONString(group);

		String usersjson1 = JSON.toJSONString(users, SerializerFeature.BeanToArray);

		System.out.println(userjson);
//		System.out.println(usersjson);
//		System.out.println(usersjson1);
	}

}
