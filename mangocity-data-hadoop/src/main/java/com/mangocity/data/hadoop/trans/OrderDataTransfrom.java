package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericRecord;

public class OrderDataTransfrom extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
	
}
