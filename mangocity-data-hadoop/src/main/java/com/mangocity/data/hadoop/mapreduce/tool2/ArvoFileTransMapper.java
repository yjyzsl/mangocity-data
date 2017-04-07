package com.mangocity.data.hadoop.mapreduce.tool2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransMapper<T> extends Mapper<AvroKey<T>,NullWritable, Text, IntWritable> {
	
	
	private IntWritable valueLong = new IntWritable();
	private Text keyLong = new Text();
	private AtomicInteger count = new AtomicInteger(0);
	
	@Override
	protected void setup(
			Mapper<AvroKey<T>, NullWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}


	@Override
	protected void cleanup(
			Mapper<AvroKey<T>, NullWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("count:"+count.get());
		super.cleanup(context);
	}

	@Override
	protected void map(AvroKey<T> valve,NullWritable key,Context context) throws  InterruptedException, IOException {
		count.incrementAndGet();
		T bean = valve.datum();
		if(bean==null){
			return ;
		}
		GenericRecord genericRecord = null;
		if(bean instanceof Record){
			genericRecord = (Record)bean;
			Object rowid = genericRecord.get("sourcerowid");
			Long sourcerowid = NumberUtils.toLong(rowid.toString());
			keyLong.set(String.valueOf(sourcerowid));
			valueLong.set(1);
			context.write(keyLong, valueLong);
		}
		
	}
	

	
	
}

