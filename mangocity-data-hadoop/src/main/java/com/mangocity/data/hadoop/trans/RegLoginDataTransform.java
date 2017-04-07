package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONObject;

/**
 *	注册登录数据转化
 * @author shilei
 * @date 2016年5月31日 下午2:15:47 
 */
public class RegLoginDataTransform extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
	
}

