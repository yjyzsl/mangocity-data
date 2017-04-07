package com.mangocity.data.hadoop.mapreduce.tool3;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.hadoop.util.HadoopConstants;
import com.mangocity.data.hadoop.util.VfsUtils;


/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:51:49 
 */
public class ArvoFileTransMapper<T> extends Mapper<AvroKey<T>,NullWritable, AvroKey<T>, NullWritable> {
	
	private static Logger logger = LoggerFactory.getLogger(ArvoFileTransMapper.class);
	
	private AvroKey<T> avroKey = new AvroKey<T>();
//	private LongWritable keyLong = new LongWritable();
	private AtomicInteger count = new AtomicInteger(0);
	 
	private Map<Long,Integer> cacheMap = new ConcurrentHashMap<Long,Integer>();
	private Set<Integer> flagSet = new HashSet<Integer>();
	private String reduceOutPath;
	
	@Override
	protected void setup(
			Mapper<AvroKey<T>,NullWritable, AvroKey<T>, NullWritable>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		reduceOutPath = context.getConfiguration().get(HadoopConstants.REDUCE_OUT_PATH);
//		List<String> rowIds = VfsUtils.readFileContentLines(HadoopConstants.REPLACEMENT_FS_VALUE+"/test/testdata/part-r-00000",HadoopConstants.UTF_8,logger);
//		String[] tmp = null;
//		Long id = null;
//		Integer count = null;
//		for(String rowid:rowIds){
//			tmp = StringUtils.split(rowid, "\t");
//			if(tmp.length==2){
//				id = NumberUtils.toLong(tmp[0]);
//				count = NumberUtils.toInt(tmp[1]);
//				if(id>0 && count>0){
//					cacheMap.put(id, count);
//				}else{
//					System.out.println(rowid);
//				}
//			}
//		} 
//		System.out.println("rowid size:"+rowIds.size());
		System.out.println("cacheMap size:"+cacheMap.size());
	}


	@Override
	protected void cleanup(
			Mapper<AvroKey<T>,NullWritable, AvroKey<T>, NullWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("count:"+count.get());
		System.out.println("cacheMap size:"+cacheMap.size());
		System.out.println("flagSet size:"+flagSet.size());
		cacheMap.clear();
		cacheMap = null;
		flagSet.clear();
		flagSet = null;
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
			if(compare(genericRecord)){
				context.write(valve, NullWritable.get());
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
	
	public static void main(String[] args) {
		List<String> rowIds = VfsUtils.readFileContentLines(HadoopConstants.REPLACEMENT_FS_VALUE+"/test/testdata/data/part-r-00000",HadoopConstants.UTF_8,logger);
		for(int i=0;i<20;i++){
			String rowid = rowIds.get(i);
			System.out.println(rowid);
			String[] tmp = StringUtils.split(rowid, "\t");
			if(tmp.length==2){
				System.out.println("id:"+tmp[0]+",count:"+tmp[1]);
			}
		} 
		System.out.println(rowIds.size());
		 
	}
	

	
	
}

