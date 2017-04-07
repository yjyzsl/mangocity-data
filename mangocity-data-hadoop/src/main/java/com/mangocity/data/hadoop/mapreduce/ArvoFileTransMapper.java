package com.mangocity.data.hadoop.mapreduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.hadoop.trans.DataTransfrom;
import com.mangocity.data.hadoop.util.HadoopConstants;


/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransMapper<T> extends Mapper<AvroKey<T>,NullWritable, Text, AvroValue<T>> {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ArvoFileTransMapper.class);
	
	private AvroValue<T> avroValue = new AvroValue<T>();
	private Text keyText = new Text();
	private String reduceOutPath;
	private AtomicInteger count = new AtomicInteger(0);
	
	@Override
	protected void setup(
			Mapper<AvroKey<T>, NullWritable, Text, AvroValue<T>>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		reduceOutPath = context.getConfiguration().get(HadoopConstants.REDUCE_OUT_PATH);
	}


	@Override
	protected void cleanup(
			Mapper<AvroKey<T>, NullWritable, Text, AvroValue<T>>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void map(AvroKey<T> valve,NullWritable key,Context context) throws  InterruptedException, IOException {
		count.incrementAndGet();
		T bean = valve.datum();
		//System.out.println(lzUserOperaTionLogBean);
		if(bean==null){
			return ;
		}
		GenericRecord genericRecord = null;
		if(bean instanceof Record){
			genericRecord = (Record)bean;
		}
		String schemaName = genericRecord.getSchema().getName();
		DataTransfrom dataTransfrom = DataTransfrom.getDataTransfrom(schemaName);
		if(dataTransfrom == null){
			LOGGER.error("没有找到 name:"+schemaName+"为schema的dataTransfrom");
			return ;
		}
		try {
			genericRecord = dataTransfrom.handleData(genericRecord);
			if(genericRecord == null){
				return ;
			}
		} catch (Exception e) {
			LOGGER.error("转化发生错误",e);
		}
		//reduceOutPath = context.getConfiguration().get(HadoopConstants.REDUCE_OUT_PATH);
		avroValue.datum(bean);
		keyText.set(reduceOutPath);
		context.write(keyText, avroValue);
	}
	

	
	
}

