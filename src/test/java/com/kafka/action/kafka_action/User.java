package com.kafka.action.kafka_action;

import java.util.Date;

import org.apache.kafka.common.config.types.Password;

import com.alibaba.fastjson.annotation.JSONField;

public class User {

	@JSONField(name = "ID", serialize = false)
	private int id;
	@JSONField(name = "FULL NAME", ordinal = 1)
	private String name;

	@JSONField(name = "AGE", ordinal = 2)
	private int age;

	@JSONField(name = "PASS WORD", ordinal = 3, serialize = false, deserialize = false)
	private Password password;

	@JSONField(name = "BIRTHDAY", format = "yyyy/mm/dd")
	private Date birthofday;

	@JSONField(name = "MEMO", serializeUsing = UserSerialize.class)
	private String memo;

	public User(int id, String name, int age, Password password, Date birthofday, String memo) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
		this.password = password;
		this.birthofday = birthofday;
		this.memo = memo;
	}

	public String getMemo() {
		return memo;
	}

	public void setMemo(String memo) {
		this.memo = memo;
	}

	public User(int id, String name, int age, Password password, Date birthofday) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
		this.password = password;
		this.birthofday = birthofday;
	}

	public Date getBirthofday() {
		return birthofday;
	}

	public void setBirthofday(Date birthofday) {
		this.birthofday = birthofday;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public User(int id, String name, int age, Password password) {
		super();
		this.id = id;
		this.name = name;
		this.age = age;
		this.password = password;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Password getPassword() {
		return password;
	}

	public void setPassword(Password password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + ", age=" + age + ", password=" + password + ", birthofday="
				+ birthofday + ", memo=" + memo + "]";
	}

}
