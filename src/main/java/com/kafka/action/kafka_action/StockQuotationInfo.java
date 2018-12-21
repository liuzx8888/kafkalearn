package com.kafka.action.kafka_action;

import java.io.Serializable;

public class StockQuotationInfo implements Serializable {
	
	private static final long serialVersionUID = 1L;
	 /*股票代码*/
	private  String  stockCode;
	 /*股票名称*/
	private  String  stockName;	
	 /*交易时间*/
	private  long    tradeTime;	
	private  float   preClosePrice;
	private  float   openPrice;
	private  float   currentPrice;	
	

}
