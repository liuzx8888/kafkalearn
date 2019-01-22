package com.kafka.action.kafka_action;

import java.util.List;

import org.apache.kafka.common.config.types.Password;

public class UserGroup {
	
	private  int Groupid;
	private  String GroupName;	
	private  List<User> users;
	public UserGroup(int groupid, String groupName, List<User> users) {
		super();
		Groupid = groupid;
		GroupName = groupName;
		this.users = users;
	}
	@Override
	public String toString() {
		return "UserGroup [Groupid=" + Groupid + ", GroupName=" + GroupName + ", users=" + users + "]";
	}
	public int getGroupid() {
		return Groupid;
	}
	public void setGroupid(int groupid) {
		Groupid = groupid;
	}
	public String getGroupName() {
		return GroupName;
	}
	public void setGroupName(String groupName) {
		GroupName = groupName;
	}
	public List<User> getUsers() {
		return users;
	}
	public void setUsers(List<User> users) {
		this.users = users;
	}
	
	
	
	
	

}
