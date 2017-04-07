package com.mangocity.data.hadoop.mapreduce.tool3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransReducer<T> extends Reducer<AvroKey<T>, NullWritable, AvroKey<T>, NullWritable>{
	
	private AvroKey<T> outKey = new AvroKey<T>();
	private AtomicInteger count = new AtomicInteger(0);
	
	private Set<Integer> flagSet = new HashSet<Integer>();
	
	@Override
	protected void setup(Reducer<AvroKey<T>, NullWritable, AvroKey<T>, NullWritable>.Context context)throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Reducer<AvroKey<T>, NullWritable, AvroKey<T>, NullWritable>.Context context)throws IOException, InterruptedException {
		System.out.println("count:"+count);
		System.out.println("flagSet size:"+flagSet.size());
		flagSet.clear();
		super.cleanup(context);
	}

	@Override
	public void reduce(AvroKey<T> key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		T t = key.datum();
		GenericRecord genericRecord = null;
		if(t instanceof Record){
			genericRecord = (Record)t;
			boolean flag = compare(genericRecord);
			if(flag){
				context.write(key, NullWritable.get());
			}
		}
	}
	
	public boolean compare(GenericRecord genericRecord){
		Object rowid = genericRecord.get("sourcerowid");
		Object recvTimeObj = genericRecord.get("recvTime");
		Object timeObj = genericRecord.get("time");
		Object deviceIdObj = genericRecord.get("deviceId");
		Object viewTypeObj = genericRecord.get("viewType");
		StringBuilder keyBuf = new StringBuilder();
		if(rowid!=null && StringUtils.isNotBlank(rowid.toString())){
			keyBuf.append(rowid.toString());
		}
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
		if(flagSet.contains(keyHash)){
			return false;
		}else{
			flagSet.add(keyHash);
			return true;
		}
	}
	
}

