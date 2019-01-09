package com.kafka.action.kafka_action;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        final String BROKER_LIST = "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092";

        System.out.println(Arrays.asList(BROKER_LIST.split(",")));
        
        System.out.println(Arrays.asList(StringUtils.split(BROKER_LIST, ",")));
        
        
        
        
    }

}
