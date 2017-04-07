package com.mangocity.data.hadoop;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author shilei
 * @date 2016年4月29日 上午11:12:10 
 */
public class AvroUtil {

	private final static Logger LOGGER = Logger.getLogger(AvroUtil.class);
	
	/**
	 * 将集合Map转化为集合GenericRecord对象
	 *
	 * @author shilei
	 * @date 2016年4月29日 上午11:36:58
	 * @param recordMapList
	 * @param schema
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static List<GenericRecord> createRecordByList(List<Map> recordMapList,Schema schema){
		List<GenericRecord> genericRecordList = null;
		if(CollectionUtils.isEmpty(recordMapList)){
			return genericRecordList;
		}
		genericRecordList = new ArrayList<GenericRecord>(recordMapList.size());
		GenericRecord genericRecord = null;
		for(Map<String,Object> recordMap:recordMapList){
			genericRecord = createRecordByMap(recordMap,schema);
			if(genericRecord!=null){
				genericRecordList.add(genericRecord);
			}
		}
		return genericRecordList;
	}
	
	/**
	 * 将Map转化为GenericRecord对象
	 *
	 * @author shilei
	 * @date 2016年4月29日 上午11:35:26
	 * @param recordMap
	 * @param schema
	 * @return
	 */
	public static GenericRecord createRecordByMap(Map<String,Object> recordMap,Schema schema){
		GenericRecord genericRecord = null;
		if(MapUtils.isEmpty(recordMap)){
			return genericRecord;
		}
		genericRecord = new GenericData.Record(schema);
		List<Field> fieldList = schema.getFields();
		String fieldName = null;
		for (Field field : fieldList) {
			fieldName = field.name();
			if(recordMap.containsKey(fieldName)){
				genericRecord.put(fieldName, recordMap.get(fieldName));
			}
		}
		return genericRecord;
	}
	
	/**
	 * 集合Map转化成Arvo格式的集合bean
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午12:42:41
	 * @param recordMapList
	 * @param clazz
	 * @param schema
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> List<T> createBeanByList(List<Map> recordMapList,Class<T> clazz,Schema schema) throws InstantiationException, IllegalAccessException, InvocationTargetException{
		List<T> tList = null;
		if(CollectionUtils.isEmpty(recordMapList)){
			return tList;
		}
		tList = new ArrayList<T>(recordMapList.size());
		T t = null;
		for (Map<String,Object> recordMap : recordMapList) {
			t = createBeanByMap(recordMap,clazz,schema);
			if(t != null){
				tList.add(t);
			}
		}
		return tList;
	}
	
	/**
	 * Map转化成Arvo格式的bean
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午12:44:05
	 * @param recordMap
	 * @param clazz
	 * @param schema
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public static <T> T createBeanByMap(Map<String,Object> recordMap,Class<T> clazz,Schema schema) throws InstantiationException, IllegalAccessException, InvocationTargetException{
		T t = clazz.newInstance();
		List<Field> fieldList = schema.getFields();
		String fieldName = null;
		Object fieldValue = null;
		for(Field field:fieldList){
			fieldName = field.name();
			if(!recordMap.containsKey(fieldName)){
				continue;
			}
			fieldValue = recordMap.get(fieldName);
			BeanUtils.setProperty(t, fieldName, fieldValue);
		}
		return t;
	}

	public static <T> List<T> readAvroFileToBean(File file,Class<T> clazz){
		return readAvroFileToBean(file,null,clazz);
	}
	
	public static <T> List<T> readAvroFileToBean(SeekableInput seekableInput,Class<T> clazz){
		return readAvroFileToBean(null,seekableInput,clazz);
	}
	
	public static <T> List<T> readAvroFileToBean(File file,SeekableInput seekableInput,Class<T> clazz){
		DataFileReader<T> dataFileReader = null;
		List<T> tList = null;
		try {
			DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
			if(file!=null && file.exists() && file.isFile()){
				dataFileReader = new DataFileReader<T>(file, datumReader);
			}else if(seekableInput!=null){
				dataFileReader = new DataFileReader<T>(seekableInput, datumReader);
			}else{
				return tList;
			}
			tList = new ArrayList<T>();
			T t = null;
			while (dataFileReader.hasNext()) {
				t = dataFileReader.next();
				tList.add(t);
			}
		} catch (Exception e) {
			LOGGER.error("read avro file to bean error clazz:"+clazz, e);
		} finally{
			IOUtils.closeQuietly(dataFileReader);
		}
		return tList;
	}
	
	public static List<GenericRecord> readAvroFileToRecord(File file,Schema schema){
		return readAvroFileToRecord(file,null,schema);
	}
	
	public static List<GenericRecord> readAvroFileToRecord(SeekableInput seekableInput,Schema schema){
		return readAvroFileToRecord(null,seekableInput,schema);
	}
	
	public static List<GenericRecord> readAvroFileToRecord(File file,SeekableInput seekableInput,Schema schema){
		DataFileReader<GenericRecord> dataFileReader = null;
		List<GenericRecord> tList = null;
		try {
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			if(file!=null && file.exists() && file.isFile()){
				dataFileReader = new DataFileReader<GenericRecord>(file,datumReader);
			}else if(seekableInput!=null){
				dataFileReader = new DataFileReader<GenericRecord>(seekableInput,datumReader);
			}else{
				return tList;
			}
			tList = new ArrayList<GenericRecord>();
			GenericRecord t = null;
			while (dataFileReader.hasNext()) {
				t = dataFileReader.next();
				tList.add(t);
			}
		} catch (Exception e) {
			LOGGER.error("read avro file to bean error ", e);
		} finally{
			IOUtils.closeQuietly(dataFileReader);
		}
		return tList;
	}
	
	
	/**
	 * 写实体bean到arvo文件中去
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午1:04:30
	 * @param datas
	 * @param clazz
	 * @param schema
	 * @param file
	 */
	public static <T> boolean writerBeanToAvroFile(List<T> datas,Class<T> clazz,Schema schema,File file){
		boolean flag = false;
		DataFileWriter<T> dataFileWriter = null;
		if(CollectionUtils.isEmpty(datas)){
			return flag;
		}
		try {
			DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
			dataFileWriter = new DataFileWriter<T>(datumWriter);
			dataFileWriter.create(schema, file);
			for (T t : datas) {
				dataFileWriter.append(t);
			}
			dataFileWriter.flush();
			flag = true;
		} catch (Exception e) {
			LOGGER.error("writer bean to avro file  error clazz:"+clazz, e);
			flag = false;
		} finally{
			IOUtils.closeQuietly(dataFileWriter);
		}
		return flag;
	}
	
	/**
	 * 增量添加对象到文件中
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午1:11:55
	 * @param datas
	 * @param clazz
	 * @param schema
	 * @param file
	 */
	public static <T> boolean appendBeanToAvroFile(List<T> datas,Class<T> clazz,Schema schema,File file){
		boolean flag = false;
		DataFileWriter<T> dataFileWriter = null;
		if(CollectionUtils.isEmpty(datas)){
			return flag;
		}
		try {
			//读取远avro文件中的对象
			List<T> originalList = readAvroFileToBean(file,clazz);
			DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
			dataFileWriter = new DataFileWriter<T>(datumWriter);
			dataFileWriter.create(schema, file);
			if(CollectionUtils.isNotEmpty(originalList)){
				//先添加源始文件中的对象
				for (T t : originalList) {
					dataFileWriter.append(t);
				}
				originalList.clear();
				originalList = null;
			}
			// 然后添加新对象
			for (T t : datas) {
				dataFileWriter.append(t);
			}
			dataFileWriter.flush();
			flag = true;
		} catch (Exception e) {
			LOGGER.error("writer bean to avro file  error clazz:"+clazz, e);
			flag = false;
		} finally{
			IOUtils.closeQuietly(dataFileWriter);
		}
		return flag;
	}
	
	/**
	 * 添加其他arvo格式文件到目标文件中，并增加arvo格式的对象
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午4:01:58
	 * @param datas arvo格式的对象
	 * @param clazz
	 * @param schema
	 * @param targetFile 目标文件
	 * @param otherFileInput 其他文件的输入流
	 */
	public static <T> boolean appendFromFileAndBeanToAvroFile(List<T> datas,Class<T> clazz,Schema schema,File targetFile){
		return appendFromFileAndBeanToAvroFile(datas,clazz,schema,targetFile,null);
	}
	
	/**
	 * 添加其他arvo格式文件到目标文件中，并增加arvo格式的对象
	 * @author shilei
	 * @date 2016年4月29日 下午4:01:58
	 * @param datas arvo格式的对象
	 * @param clazz
	 * @param schema
	 * @param targetFile 目标文件
	 * @param otherFileInput 其他文件的输入流
	 */
	public static <T> boolean appendFromFileAndBeanToAvroFile(List<T> datas,Class<T> clazz,Schema schema,File targetFile,InputStream otherFileInput){
		boolean flag = false;
		DataFileWriter<T> dataFileWriter = null;
		if(CollectionUtils.isEmpty(datas)){
			return flag;
		}
		try {
			DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(clazz);
			dataFileWriter = new DataFileWriter<T>(datumWriter);
			dataFileWriter.create(schema, targetFile);
			
			DatumReader<T> datumReader = new SpecificDatumReader<T>(clazz);
			DataFileStream<T> otherFile = new DataFileStream<T>(otherFileInput, datumReader);
			if(otherFileInput!=null){
				dataFileWriter.appendAllFrom(otherFile, true);
			}
			for (T t : datas) {
				dataFileWriter.append(t);
			}
			dataFileWriter.flush();
			flag = true;
		} catch (Exception e) {
			LOGGER.error("writer bean to avro file  error clazz:"+clazz, e);
			flag = false;
		} finally{
			IOUtils.closeQuietly(dataFileWriter);
		}
		return flag;
	}
	
	/**
	 * 增量添加对象到文件中
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午1:14:14
	 * @param datas
	 * @param schema
	 * @param file
	 */
	public static boolean appendRecordToAvroFile(List<GenericRecord> datas,Schema schema,File file){
		boolean flag = false;
		DataFileWriter<GenericRecord> dataFileWriter = null;
		if(CollectionUtils.isEmpty(datas)){
			return flag;
		}
		try {
			//读取远avro文件中的对象
			List<GenericRecord> originalList = readAvroFileToRecord(file, schema);
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(schema, file);
			if(CollectionUtils.isNotEmpty(originalList)){
				for (GenericRecord genericRecord : originalList) {
					dataFileWriter.append(genericRecord);
				}
			}
			for (GenericRecord genericRecord : datas) {
				dataFileWriter.append(genericRecord);
			}
			dataFileWriter.flush();
			flag = true;
		} catch (Exception e) {
			LOGGER.error("writer genericRecord to avro file  error ", e);
			flag = false;
		} finally{
			IOUtils.closeQuietly(dataFileWriter);
		}
		return flag;
	}
	
	
	/**
	 * 写GenericRecord对象到arvo文件中去
	 *
	 * @author shilei
	 * @date 2016年4月29日 下午1:04:58
	 * @param datas
	 * @param schema
	 * @param file
	 */
	public static boolean writerRecordToAvroFile(List<GenericRecord> datas,Schema schema,File file){
		boolean flag = false;
		DataFileWriter<GenericRecord> dataFileWriter = null;
		if(CollectionUtils.isEmpty(datas)){
			return flag;
		}
		try {
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(schema, file);
			for (GenericRecord genericRecord : datas) {
				dataFileWriter.append(genericRecord);
			}
			dataFileWriter.flush();
			flag = true;
		} catch (Exception e) {
			LOGGER.error("writer genericRecord to avro file  error ", e);
			flag = false;
		} finally{
			IOUtils.closeQuietly(dataFileWriter);
		}
		return flag;
	}
	
//	@Test
//	public static void generateAvroJavaFile(){
//		Tool tool = new SpecificCompilerTool();
//	      if (tool != null) {
//	    	  List<String> arr = new ArrayList<String>();
//	    	  arr.add("schema");
//	    	  arr.add("E:/shilei/workspace/hadoop/src/main/resources/user_option_action_log.avsc");
//	    	  arr.add("E:\\shilei\\workspace\\hadoop\\src\\main\\java");
//	    	  try {
//				tool.run(System.in, System.out, System.err, arr);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//	      }
//	}
	
	
}

