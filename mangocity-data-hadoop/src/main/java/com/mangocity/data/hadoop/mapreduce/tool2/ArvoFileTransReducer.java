package com.mangocity.data.hadoop.mapreduce.tool2;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransReducer extends Reducer<Text, IntWritable, Text,IntWritable>{
	
	private IntWritable outKey = new IntWritable();
	private AtomicInteger count = new AtomicInteger(0);
	
	@Override
	protected void setup(Reducer<Text, IntWritable, Text,IntWritable>.Context context)throws IOException, InterruptedException {
		super.setup(context);
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text,IntWritable>.Context context)throws IOException, InterruptedException {
		System.out.println("count:"+count);
		super.cleanup(context);
	}

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
			count.incrementAndGet();
			if(count.get()%10000==0){
				System.out.println("key:"+key+",sum:"+sum);
			}
		}
		outKey.set(sum);
		context.write(key, outKey);
	}
	
}

