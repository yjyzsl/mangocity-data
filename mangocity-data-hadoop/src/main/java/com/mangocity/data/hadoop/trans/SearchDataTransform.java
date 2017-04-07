package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericRecord;

/**
 *	搜索信息数据转化
 * @author shilei
 * @date 2016年5月31日 下午1:35:08 
 */
public class SearchDataTransform extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
	
}

