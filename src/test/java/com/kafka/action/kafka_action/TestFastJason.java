package com.kafka.action.kafka_action;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TestFastJason {

	// 解析
	
	public void test1() {
		// 对象嵌套数组嵌套对象
		String json1 = "{'id':1,'name':'JAVAEE-1703','stus':[{'id':101,'name':'刘铭','age':16},{'id':102,'name':'刘铭22','age':18}]}";
		// 数组
		String json2 = "['北京','天津','杭州']";

		// 静态方法
		Grade grade = JSON.parseObject(json1, Grade.class);
		System.out.println(grade);
		// 2、
		List<String> list = JSON.parseArray(json2, String.class);
		System.out.println(list);
	}
	
	   //生成
    
    public void test2(){
        ArrayList<Student> list=new ArrayList<>();
        for(int i=1;i<3;i++){
            list.add(new Student(101+i, "码子", 20+i));
        }
        Grade grade=new Grade(100001,"张三", list);
        String json=JSON.toJSONString(grade);
        System.out.println(json);
    }
    
    
    /**
     * java对象转 json字符串 
     */
    @Test
    public void  objectTOJson() {
    	//Java 类 转换成 json字符串 
    	User user = new User("test1","112233");
    	String userjSon =  JSON.toJSONString(user);
    	System.out.println(userjSon);
    	
    	//List 转  json字符串 
    	List<User> users = new LinkedList<User>();
    	users.add( new User("test2","112233"));
    	users.add( new User("test3","112233"));   	
    	users.add( new User("test4","112233"));     
    	String usersjSon=JSON.toJSONString(users);
     	System.out.println(usersjSon);	
     	
     	
    	//复杂的Java类转 jason
     	UserGroup userGroup = new UserGroup("usergroup",users);
     	String  userGroupJson = JSON.toJSONString(userGroup);
     	System.out.println(userGroupJson.toString());
  
     	
     	
    	List<User> users1 = new LinkedList<User>();	
     	User user2 =   JSON.parseObject(userjSon,User.class);
     	
     	users1 = JSON.parseArray(usersjSon, User.class);
     	
     	System.out.println(user2.toString());
     	System.out.println(users1.toString());
    }
    
    
    


}
