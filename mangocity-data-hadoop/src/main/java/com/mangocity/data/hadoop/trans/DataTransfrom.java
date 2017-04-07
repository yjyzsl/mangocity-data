package com.mangocity.data.hadoop.trans;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author shilei
 * @date 2016年6月30日 上午8:24:39 
 */
public abstract class DataTransfrom {

	/**
	 * 每个数据处理的Transform写好之后就会在此处注册
	 */
	private static final Map<String,DataTransfrom> transformMap = new HashMap<String,DataTransfrom>();
	
	static {
		transformMap.put("LzUserOperaTionLogBean",new LzDataTransfrom());
		transformMap.put("UserOperaTionLogBean",new UolDataTransfrom());
		transformMap.put("EntryData",new EntryDataTransfrom());
		transformMap.put("ClickData",new ClickDataTransfrom());
		transformMap.put("SearchData",new SearchDataTransform());
		transformMap.put("ProductDetailData",new ProductDetailDataTransfrom());
		transformMap.put("RegLoginData",new RegLoginDataTransform());
		transformMap.put("OrderData",new OrderDataTransfrom());
		transformMap.put("PersonEditData",new PersonalModifDataTransform());
		transformMap.put("LeaveData",new LeaveDataTransfrom());
	}
	
	public static DataTransfrom getDataTransfrom(String name){
		return transformMap.get(name);
	}
	
	public abstract GenericRecord handleData(GenericRecord record);

	public String getStringValue(GenericRecord record,String name){
		String value = null;
		Object obj = record.get(name);
		if(obj != null){
			value = obj.toString();
		}
		return value;
	}
	
	public void putValue(GenericRecord record,String name,Object value){
		record.put(name, value);
	}
}

