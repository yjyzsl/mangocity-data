package com.mangocity.data.hadoop.mapreduce.tool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mangocity.data.hadoop.util.HadoopConstants;

/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransReducer<T> extends Reducer<Text, AvroValue<T>, AvroKey<T>, NullWritable>{
	
	private AvroKey<T> outKey = new AvroKey<T>();
	private AvroMultipleOutputs mos;
	private AtomicInteger count = new AtomicInteger(0);
	
	@Override
	protected void setup(Reducer<Text, AvroValue<T>, AvroKey<T>, NullWritable>.Context context)throws IOException, InterruptedException {
		mos = new AvroMultipleOutputs(context);
	}
	
	@Override
	protected void cleanup(Reducer<Text, AvroValue<T>, AvroKey<T>, NullWritable>.Context context)throws IOException, InterruptedException {
		mos.close();
		System.out.println("count:"+count);
		super.cleanup(context);
	}

	@Override
	public void reduce(Text key, Iterable<AvroValue<T>> values,Context context) throws IOException, InterruptedException {
		T lzUserOperaTionLogBean = null;
		String baseOutputPath = key.toString();
		for (AvroValue<T> avroValue : values) {
			count.incrementAndGet();
			lzUserOperaTionLogBean = avroValue.datum();
			outKey.datum(lzUserOperaTionLogBean);
			mos.write(HadoopConstants.NAMED_OUTPUT, outKey, NullWritable.get(), baseOutputPath);
		}
	}
}

