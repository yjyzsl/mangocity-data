package com.mangocity.data.hadoop.maped;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMultipleOutputs;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileMergeReducer extends MapReduceBase implements Reducer<AvroKey<Utf8>, AvroValue<GenericRecord>,Text, IntWritable>{

	private AvroMultipleOutputs mos;
	private AtomicInteger count = new AtomicInteger();
	
	@Override
	public void close() throws IOException {
		mos.close();
		System.out.println("reduce count:"+count);
		super.close();
	}

	@Override
	public void configure(JobConf job) {
		mos = new AvroMultipleOutputs(job);
		super.configure(job);
	}


	@Override
	public void reduce(AvroKey<Utf8> key,Iterator<AvroValue<GenericRecord>> values,OutputCollector<Text, IntWritable> output,Reporter reporter) throws IOException {
		 AvroWrapper<GenericRecord> record = null;
		 String[] outputPaths = key.datum().toString().split("#");
		 String namedOutput = outputPaths[0];
		 String baseOutputPath = outputPaths[1];
		 while(values.hasNext()){
			 record = values.next();
			 //mos.write("test", outKey, NullWritable.get(), baseOutputPath);
			 mos.getCollector(namedOutput,reporter).collect(record.datum());
			 count.incrementAndGet();
		 }
		 
	}

}

