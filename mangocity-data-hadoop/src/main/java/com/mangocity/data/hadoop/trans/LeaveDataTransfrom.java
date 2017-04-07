package com.mangocity.data.hadoop.trans;

import org.apache.avro.generic.GenericRecord;

public class LeaveDataTransfrom extends DataTransfrom{

	@Override
	public GenericRecord handleData(GenericRecord record) {
		return record;
	}
}