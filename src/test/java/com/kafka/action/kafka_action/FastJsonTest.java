package com.kafka.action.kafka_action;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.config.types.Password;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.SerializationFeature;

public class FastJsonTest {

	// String json="{\"table\":\"DBO.TAB\",\"op_type\":\"I\",\"op_ts\":\"2019-01-21
	// 14:05:18.446967\",\"current_ts\":\"2019-01-21T22:26:10.481001\",\"pos\":\"00000000400000050285\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":211170,\"BIRTHDATE\":\"2019-01-22
	// 00:58:17.903000000\",\"AGE\":99,\"NAME\":\"kkrrr\"}" ;

	public void ToJsonString() {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String date = dateFormat.format(new Date());

		User user1 = new User(1, "test1", 18, new Password("112233"), date,
				"{\"id\":\"5001\",\"name\":\"Jobs\",\"age\":\"18\",\"BIRTHDAY\":\"2019-01-23\",\"MEMO\":\"444555\"}");
		User user2 = new User(2, "test2", 18, new Password("112233"), date, "2222");
		User user3 = new User(3, "test3", 18, new Password("112233"), date, "3333");
		List<User> users = new LinkedList<User>();
		users.add(user1);
		users.add(user2);
		users.add(user3);
		UserGroup group = new UserGroup(1, "test1", users);

		String userjson = JSON.toJSONString(user1);
		String usersjson = JSON.toJSONString(group);
		String usersjson1 = JSON.toJSONString(users, SerializerFeature.BeanToArray);

		User user4 = JSON.parseObject(userjson, User.class);
		System.out.println(userjson);

		// System.out.println(usersjson);
		// System.out.println(usersjson1);
		// System.out.println(user4.toString());

		String jsonuser = "{\"id\":\"5001\",\"name\":\"Jobs\",\"age\":\"18\",\"BIRTHDAY\":\"2019-01-23\",\"MEMO\":\"444555\"}";
		User user5 = JSON.parseObject(jsonuser, User.class);
		System.out.println(user5.toString());

		String jsonVal0 = "{\"id\":\"5001\",\"name\":\"jobs\",\"demo\":\"{}}";
		String jsonVal1 = "{\"id\":5382,\"user\":\"Mary\"}";
		String jsonVal2 = "{\"id\":2341,\"person\":\"Bob\"}";

		Model obj0 = JSON.parseObject(jsonVal0, Model.class);
		assertEquals(5001, obj0.id);
		assertEquals("Jobs", obj0.name);
		assertEquals("Jobs", obj0.name);
		System.out.printf("obj0:%s", obj0);
		//
		// Model obj1 = JSON.parseObject(jsonVal1, Model.class);
		// assertEquals(5382, obj1.id);
		// assertEquals("Mary", obj1.name);
		// System.out.println(obj1);
		//
		// Model obj2 = JSON.parseObject(jsonVal2, Model.class);
		// assertEquals(2341, obj2.id);
		// assertEquals("Bob", obj2.name);
		// System.out.println(obj2);

		String id = (String) JSONPath.eval(userjson, "$.id");
		System.out.printf("id=%s", id);
		System.out.println("");

	}

	@Test
	public void test_entity() throws Exception {
		Entity entity1 = new Entity(1, "name1", 111);
		Entity entity2 = new Entity(2, "name2", 222);
		Entity entity3 = new Entity(3, "name3", 333);

		List<Entity> entities = new LinkedList<Entity>();
		entities.add(entity1);
		entities.add(entity2);
		entities.add(entity3);

		System.out.println("\n-----------------------读取对象中的值------------------------------");
		System.out.println("JSONPath.eval(entity1, \"$.name\") = " + JSONPath.eval(entity1, "$.name"));
		System.out.println("JSONPath.contains(entity1, \"$.id\") = " + JSONPath.contains(entity1, "$.id"));
		System.out.println("JSONPath.containsValue(entity1, \"$.name\", \"name1\") = "
				+ JSONPath.containsValue(entity1, "$.name", "name1"));
		System.out.println("JSONPath.containsValue(entity, \"$.value\", entity.getValue()) = "
				+ JSONPath.containsValue(entity1, "$.value", entity1.getValue()));

		System.out.println("JSONPath.size(entity1, \"$\")  = " + JSONPath.size(entity1, "$"));
		System.out.println("JSONPath.size(new Object[], \"$\") = " + JSONPath.size(new Object[100], "$"));

		System.out.println("\n-----------------------读取集合元素中的值------------------------------");
		System.out.println("JSONPath.eval(entities, \"$.name\") = " + JSONPath.eval(entities, "$.name"));
		System.out.println("JSONPath.contains(entities, \"$.id\") = " + JSONPath.contains(entities, "$.id"));
		System.out.println("JSONPath.containsValue(entities, \"$.name\", \"name1\") = "
				+ JSONPath.containsValue(entities, "$.name", "name1"));
		System.out.println("JSONPath.containsValue(entities, \"$.value\", entity.getValue()) = "
				+ JSONPath.containsValue(entities, "$.value", entity1.getValue()));

		System.out.println("JSONPath.size(entities, \"$\")  = " + JSONPath.size(entities, "$"));
		List<Entity> result = (List<Entity>) JSONPath.eval(entities, "[1,3]");
		System.out.println("[1,2] = " + result.iterator().next().getName());

	}

	public static class Entity {
		private Integer id;
		private String name;
		private Object value;

		public Entity() {
		}

		public Entity(Integer id, String name, Object value) {
			super();
			this.id = id;
			this.name = name;
			this.value = value;
		}

		public Entity(Integer id, Object value) {
			this.id = id;
			this.value = value;
		}

		public Entity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Entity(String name) {
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public Object getValue() {
			return value;
		}

		public String getName() {
			return name;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setValue(Object value) {
			this.value = value;
		}
	}
}
