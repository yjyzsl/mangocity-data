package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.alibaba.fastjson.JSONObject;

/**
 *	会员信息修改转化
 * @author shilei
 * @date 2016年5月31日 下午2:19:44 
 */
public class PersonalModifDataTransform extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
	
}

