package com.kafka.action.kafka_action;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.inference.WilcoxonSignedRankTest;
import org.apache.kafka.common.config.types.Password;
import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.JSONWriter;
import com.alibaba.fastjson.parser.deserializer.ExtraProcessor;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class FastJsonTest {

	// String json="{\"table\":\"DBO.TAB\",\"op_type\":\"I\",\"op_ts\":\"2019-01-21
	// 14:05:18.446967\",\"current_ts\":\"2019-01-21T22:26:10.481001\",\"pos\":\"00000000400000050285\",\"primary_keys\":[\"ID\"],\"after\":{\"ID\":211170,\"BIRTHDATE\":\"2019-01-22
	// 00:58:17.903000000\",\"AGE\":99,\"NAME\":\"kkrrr\"}" ;
	@Test
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
		
        User[] users2 = new User[3];  
        users2[0] = user2;  
        users2[1] = user3;  
        users2[2] = user3;  
        String usersjson2 = JSON.toJSONString(users2);
		System.out.println("usersjson2"+usersjson2);      
		List<User> users1 = JSON.parseArray(usersjson2, User.class);
		System.out.println("users1"+users1);

		User user4 = JSON.parseObject(userjson, User.class);
		System.out.println("userjson:" + userjson);

		System.out.println("usersjson:"+usersjson);
		System.out.println("usersjson1:"+usersjson1);
		// System.out.println(user4.toString());

		String jsonuser = "{\"id\":\"5001\",\"name\":\"Jobs\",\"age\":\"18\",\"BIRTHDAY\":\"2019-01-23\",\"MEMO\":\"444555\"}";
		User user5 = JSON.parseObject(jsonuser, User.class);
		System.out.println(user5.toString());

		String jsonVal0 = "{\"id\":\"5001\",\"name\":\"jobs\",\"demo\":\"{}}";
		String jsonVal1 = "{\"id\":5382,\"user\":\"Mary\"}";
		String jsonVal2 = "{\"id\":2341,\"person\":\"Bob\"}";

		Model obj0 = JSON.parseObject(jsonVal0, Model.class);

		String id = (String) JSONPath.eval(userjson, "$.id");
		System.out.printf("id=%s", id);
		System.out.println("");

	}

	//@Test
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
		System.out.println("JSONPath.containsValue(entities, \"$.value\", entity.getValue()) = "
				+ JSONPath.containsValue(entities, "$.value", entity1.getValue()));

		System.out.println("JSONPath.size(entities, \"$\")  = " + JSONPath.size(entities, "$"));
		String string_index = "[0:" + String.valueOf(entities.size()) + "]";
		List<Entity> result = (List<Entity>) JSONPath.eval(entities, string_index);
		System.out.println(string_index + ":      " + result.get(1).getId());

		System.out.println("JSONPath.eval(entities, \"[id in (1,2)]\") = " + JSONPath.eval(entities, "[id in (1,2)]"));
		System.out.println("JSONPath.eval(entities, \"[id = 1]\") = " + JSONPath.eval(entities, "[id =1]"));

		JSONPath.set(entity1, "name", "eeexxx");
		System.out.println(JSONPath.eval(entity1, "name"));
		String[] strarr = new String[3];
		JSONPath.set(entity1, "value", new ArrayList<String>());
		JSONPath.arrayAdd(entity1, "value", "1,2,3");
		System.out.println(JSONPath.eval(entity1, "$.value"));

		Map root = Collections.singletonMap("company", //
				Collections.singletonMap("departs", //
						Arrays.asList( //
								Collections.singletonMap("id", 1001), //
								Collections.singletonMap("id", 1002), //
								Collections.singletonMap("id", 1003) //
						) //
				));

		List<Object> ids = (List<Object>) JSONPath.eval(root, "$..id");
		System.out.println(ids);

		System.out.println("\n-----------------------WriteJsonString------------------------------");
		File file = new File("D:\\json.txt");
		File file1 = new File("D:\\json1.txt");
		Entity entity4 = new Entity();
		entity4.setId(55);
		OutputStream os = new FileOutputStream(file);
		// JSON.writeJSONString(os,entity4);
		JSON.writeJSONString(os, Charset.forName("utf-8"), entity4);
		os.close();

		Writer writer = new FileWriter(file1);
		JSON.writeJSONString(writer, entity4);
		writer.close();

		System.out.println("\n-----------------------Stream------------------------------");
		JSONWriter writer1 = new JSONWriter(new FileWriter("D:\\json2.txt"));
		writer1.startArray();
		for (int i = 0; i < 1000 * 1000; ++i) {
			writer1.writeValue(new VO());
		}
		writer1.endArray();
		writer1.close();

		System.out.println("\n-----------------------ExtraProcessor------------------------------");
		ExtraProcessor processor = new ExtraProcessor() {
			public void processExtra(Object object, String key, Object value) {
				VO vo = (VO) object;
				System.out.println("key:" + key + "value:" + value);
				vo.getAttributes().put(key, value);
			}
		};

		VO vo = JSON.parseObject("{\"id\":123,\"name\":\"abc\"}", VO.class, processor);
		System.out.println(vo.getId());
		System.out.println(vo.getAttributes().get("name"));

		VO vo1 = JSON.parseObject("{\"id\":123,\"value\":\"123456\"}", VO.class, processor);
		System.out.println(vo1.getId());
		System.out.println(vo1.getAttributes().get("value"));
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
