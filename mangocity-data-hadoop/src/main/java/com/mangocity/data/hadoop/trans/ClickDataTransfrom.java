package com.mangocity.data.hadoop.trans;

import java.util.HashSet;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;


public class ClickDataTransfrom extends DataTransfrom{

	// 判断数据是否重复
	private final static Set<Integer> timeSet = new HashSet<Integer>();
	
	@Override
	public GenericRecord handleData(GenericRecord record) {
		Object recvTimeObj = record.get("recvTime");
		Object timeObj = record.get("time");
		Object deviceIdObj = record.get("deviceId");
		Object viewTypeObj = record.get("viewType");
		StringBuilder keyBuf = new StringBuilder();
		if(recvTimeObj!=null && StringUtils.isNotBlank(recvTimeObj.toString())){
			keyBuf.append(recvTimeObj.toString());
		}
		if(timeObj!=null && StringUtils.isNotBlank(timeObj.toString())){
			keyBuf.append(timeObj.toString());
		}
		if(deviceIdObj!=null && StringUtils.isNotBlank(deviceIdObj.toString())){
			keyBuf.append(deviceIdObj.toString());
		}
		if(viewTypeObj!=null && StringUtils.isNotBlank(viewTypeObj.toString())){
			keyBuf.append(viewTypeObj.toString());
		}
		int keyHash = keyBuf.toString().hashCode();
		if(timeSet.contains(keyHash)){
			return null;
		}else{
			timeSet.add(keyHash);
		}
		return record;
	}

}
