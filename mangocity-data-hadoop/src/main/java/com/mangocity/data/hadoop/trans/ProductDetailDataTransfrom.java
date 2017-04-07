package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericRecord;

public class ProductDetailDataTransfrom extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
	
}
