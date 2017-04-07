package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericRecord;

/**
 *
 * @author shilei
 * @date 2016年6月30日 下午2:39:12 
 */
public class UolDataTransfrom extends DataTransfrom {

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}

}

