package com.mangocity.data.hadoop.maped;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.mangocity.data.hadoop.trans.DataTransfrom;

/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileMergeMapper extends MapReduceBase implements Mapper<AvroWrapper<GenericRecord>, NullWritable, AvroKey<Utf8>, AvroValue<GenericRecord>> {
	
	private final static Logger LOGGER = Logger.getLogger(ArvoFileMergeMapper.class);
	
	private AvroKey<Utf8> avroKey = new AvroKey<Utf8>();
	private AvroValue<GenericRecord> avroValue = new AvroValue<GenericRecord>();
	private AtomicInteger count = new AtomicInteger();
	private String outputPath;
	
	@Override
	public void configure(JobConf job) {
		super.configure(job);
	    if(StringUtils.isNotBlank(job.get("outputPath"))){
	    	 outputPath = job.get("outputPath");
	    }
	    System.out.println("outputPath:"+outputPath);
	}
	
	@Override
	public void close() throws IOException {
		System.out.println("count:"+count);
		super.close();
	}
	
	@Override
	public void map(AvroWrapper<GenericRecord> key, NullWritable value,OutputCollector<AvroKey<Utf8>, AvroValue<GenericRecord>> output, Reporter reporter)throws IOException {
		GenericRecord genericRecord = key.datum();
		String schemaName = genericRecord.getSchema().getName();
		DataTransfrom dataTransfrom = DataTransfrom.getDataTransfrom(schemaName);
		if(dataTransfrom == null){
			LOGGER.error("没有找到 name:"+schemaName+"为schema的dataTransfrom");
			return ;
		}
		try {
			genericRecord = dataTransfrom.handleData(genericRecord);
		} catch (Exception e) {
			LOGGER.error("转化发生错误",e);
		}
		outputPath = schemaName+"#"+outputPath;
		Utf8 path = new Utf8(outputPath);
        avroKey.datum(path);
        avroValue.datum(genericRecord);
        count.incrementAndGet();
        output.collect(avroKey, avroValue);
	}
	
	
}

