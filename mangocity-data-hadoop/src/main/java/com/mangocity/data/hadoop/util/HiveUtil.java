package com.mangocity.data.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;

/**
 *
 * @author shilei
 * @date 2016年5月18日 下午12:16:00 
 */
public class HiveUtil {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(HiveUtil.class);
	
	private static HiveUtil hiveUtil;
	
	private HiveMetaStoreClient client;
	
	private Parser parser = new Schema.Parser();
	
	private HiveUtil(){
		try {
			init();
		} catch (MetaException e) {
			LOGGER.error("{}",e);
		}
	}
	
	public static HiveUtil getInstance(){
		if(hiveUtil == null){
			synchronized (HiveUtil.class) {
				if(hiveUtil!=null) {
					return hiveUtil;
				}
				hiveUtil = new HiveUtil();
			}
		}
		return hiveUtil;
	}
	
	public void init() throws MetaException{
		// 加载配置文件
		PropertiesUtil.loadConfigFile("conf/mango-hadoop.properties");
		HiveConf hiveConf = new HiveConf();
		String hiveMetastoreUris = PropertiesUtil.getValue(HadoopConstants.HIVE_METASTORE_URIS, HadoopConstants.HIVE_METASTORE_URIS_VALUE);
		String hiveMetastoreLocal = PropertiesUtil.getValue(HadoopConstants.HIVE_METASTORE_LOCAL, HadoopConstants.HIVE_METASTORE_LOCAL_VALUE);
		hiveConf.set(HadoopConstants.HIVE_METASTORE_URIS, hiveMetastoreUris);
		hiveConf.set(HadoopConstants.HIVE_METASTORE_LOCAL, hiveMetastoreLocal);
		client = new HiveMetaStoreClient(hiveConf);
	}
	
	/**
	 * 从HiveMetaStore里获取所有的表
	 *
	 * @author shilei
	 * @date 2016年5月18日 下午12:28:34
	 * @return
	 * @throws TException 
	 * @throws NoSuchObjectException 
	 */
	public Map<String,Table> getHiveTableMap() throws NoSuchObjectException, TException{
		Map<String,Table> hiveTableMap = null;
		List<String> allDatabases = client.getAllDatabases();
		Table table = null;
		if(CollectionUtils.isEmpty(allDatabases)){
			return hiveTableMap;
		}
		hiveTableMap = new HashMap<String, Table>();
		for (String dbName : allDatabases) {
			List<String> allTables = client.getAllTables(dbName);
			for (String tableName : allTables) {
				table = client.getTable(dbName, tableName);
				hiveTableMap.put(tableName, table);
			}
		}
		return hiveTableMap;
	}
	
	public Table getHiveTable(String databaseName,String tableName) throws NoSuchObjectException, TException{
		Table table = client.getTable(databaseName, tableName);
		return table;
	}
	
	public Map<String,List<Path>> getInputPathMapByTable(Table table){
		Map<String,List<Path>> inputPathMap = null;
		String executeType = getTableType(table);
		if(StringUtils.isBlank(executeType)){
			return inputPathMap;
		}
		inputPathMap = getInputPathMapByTable(table,executeType);
		return inputPathMap;
		
	}
	
	public String getTableType(Table table){
		String tableType = null;
		StorageDescriptor sd = table.getSd();
		SerDeInfo serDeInfo = sd.getSerdeInfo();
		String serializationLib = serDeInfo.getSerializationLib();
		if(!HadoopConstants.SERIALIZATION_LIB_ARVO.equals(serializationLib)){//暂时只合并arvo格式的数据
			LOGGER.warn("table {} is not arvo file.", table.getTableName());
			return tableType;
		}
		int bucketColsSize = sd.getBucketColsSize();
		int partitionKeysSize = table.getPartitionKeysSize();
		if(bucketColsSize>0 && partitionKeysSize>0){
			tableType = HadoopConstants.EXECUTE_PARTITION_BUCKET_TABLE;
		}else if(partitionKeysSize>0 && bucketColsSize==0){
			tableType = HadoopConstants.EXECUTE_PARTITION_TABLE;
		}else if(partitionKeysSize == 0 && bucketColsSize>0){
			tableType = HadoopConstants.EXECUTE_BUCKET_TABLE;
		}else{
			tableType = HadoopConstants.EXECUTE_TABLE;
		}
		return tableType;
	}
	
	public Map<String,List<Path>> getInputPathMapByTable(Table table,String executeType){
		Map<String,List<Path>> inputPathMap = new HashMap<String,List<Path>>();
		StorageDescriptor sd = table.getSd();
		String location = sd.getLocation();
		boolean checkTable = PropertiesUtil.getBooleanValue(HadoopConstants.CHECK_TABLE, HadoopConstants.CHECK_TABLE_VALUE);
		String[] tableNames = StringUtils.split(PropertiesUtil.getValue(HadoopConstants.ALLOW_TABLES, HadoopConstants.ALLOW_TABLES_VALUE), ",");
		List<String> tableNameList = Arrays.asList(tableNames);
		if(checkTable && !tableNameList.contains(table.getTableName())){
			LOGGER.info("filter table {}", table.getTableName());
			return inputPathMap;
		}
//		if(!("lz_user_option_action_log".equals(table.getTableName()) || "tmp_user_option_action_log".equals(table.getTableName()))){
////		if(!("lz_user_option_action_log".equals(table.getTableName()))){
//			LOGGER.info("filter table {}", table.getTableName());
//			return inputPathMap;
//		}
		String expectFs = PropertiesUtil.getValue(HadoopConstants.EXPECT_FS, HadoopConstants.EXPECT_FS_VALUE);
		String replacementFs = PropertiesUtil.getValue(HadoopConstants.REPLACEMENT_FS,HadoopConstants.REPLACEMENT_FS_VALUE);
		location = StringUtils.replace(location, expectFs, replacementFs);
		Path inputPath = new Path(location);
		
		Pattern	pattern = null;
		if(HadoopConstants.EXECUTE_PARTITION_TABLE.equals(executeType) || HadoopConstants.EXECUTE_PARTITION_BUCKET_TABLE.equals(executeType)){
			List<FieldSchema> partitionKeys = table.getPartitionKeys();
			StringBuilder buff = new StringBuilder();
			buff.append("^").append(location).append("/.*");
			for (FieldSchema fieldSchema : partitionKeys) {
				buff.append(fieldSchema.getName()).append("=.*/");
			}
			buff.deleteCharAt(buff.length()-1);
			buff.append("$");
			pattern = Pattern.compile(buff.toString());
		}
//		if("tmp_app_product_detail_log".equals(table.getTableName())){
//			inputPath = new Path("hdfs://10.10.4.115:9000/dsp/tmp_datas/app_hive/test/");
//			LOGGER.info("inputPath {}",inputPath);
//		}
		
		List<LocatedFileStatus> fileList = HDFSUtil.getInstance().readPathSubFile(inputPath);
        Path filePath = null;
        long length = 0L;
        long maxBlocksize = 134217728L;
        List<Path> pathList = null;
        String mapKey = null;
        
        Matcher matcher = null;
		for (LocatedFileStatus locatedFileStatus : fileList) {
			filePath = locatedFileStatus.getPath();
			if(pattern != null){
				matcher = pattern.matcher(filePath.toString());
				if(!matcher.matches()){//path不满足格式
					LOGGER.info("not marcher path : {}",pattern);
					continue;
				}
			}
			length = locatedFileStatus.getLen();
			maxBlocksize = PropertiesUtil.getNumberValue(HadoopConstants.HDFS_FILE_MAX_BLOCK_NUM, HadoopConstants.HDFS_FILE_MAX_BLOCK_NUM_VALUE) * locatedFileStatus.getBlockSize();
			if(length>=maxBlocksize){
				continue;
			}
			if(length==0){
				try {
					HDFSUtil.getInstance().deleteFile(locatedFileStatus.getPath());
				} catch (IOException e) {
					LOGGER.error("error",e);
				}
				continue;
			}
			if(HadoopConstants.EXECUTE_BUCKET_TABLE.equals(executeType) || HadoopConstants.EXECUTE_PARTITION_BUCKET_TABLE.equals(executeType)){
				// 桶表
				mapKey = filePath.getParent().toString()+"/"+StringUtils.split(filePath.getName(), "_")[0];
			}else{// 非桶表
				mapKey = filePath.getParent().toString();
			}
			
			pathList = inputPathMap.get(mapKey);
			if(pathList == null){
				pathList = new ArrayList<Path>();
				inputPathMap.put(mapKey, pathList);
			}
			pathList.add(filePath);
		}
		return inputPathMap;
	}
	
	
	public Schema getSchemaByTable(Table table){
		Schema schema = null;
		Map<String,String> parameters = table.getParameters();
		if(!parameters.containsKey(HadoopConstants.AVRO_SCHEMA_URL)){
			LOGGER.warn("table {} avro.schema.url is null",table.getTableName());
			return schema;
		}
		String avroSchemaUrl = parameters.get(HadoopConstants.AVRO_SCHEMA_URL);
		String expectFs = PropertiesUtil.getValue(HadoopConstants.EXPECT_FS, HadoopConstants.EXPECT_FS_VALUE);
		String replacementFs = PropertiesUtil.getValue(HadoopConstants.REPLACEMENT_FS,HadoopConstants.REPLACEMENT_FS_VALUE);
		if(StringUtils.contains(avroSchemaUrl, expectFs)){
			avroSchemaUrl = StringUtils.replace(avroSchemaUrl, expectFs, replacementFs);
		}else if(StringUtils.startsWith(avroSchemaUrl, "/")){
			avroSchemaUrl = StringUtils.join(replacementFs,avroSchemaUrl);
		}else if(StringUtils.isBlank(avroSchemaUrl)){
			LOGGER.warn("table {}  avro.schema.url is null",table.getTableName());
			return schema;
		}
		String avroSchemaStr = VfsUtils.readFileToString(avroSchemaUrl);
		LOGGER.debug("avroSchemaUrl {} , avroSchemaStr {}",avroSchemaUrl,avroSchemaStr);
		schema = parser.parse(avroSchemaStr);
		return schema;
	}  
	
	public static void main(String[] args) {
		try {
			Table table = HiveUtil.getInstance().getHiveTable("ods","lz_user_option_action_log");
			String type = HiveUtil.getInstance().getTableType(table);
			System.out.println(HiveUtil.getInstance().getHiveTable("ods","lz_user_option_action_log"));
			System.out.println( HiveUtil.getInstance().getInputPathMapByTable(table,type));
		} catch (TException e) {
			e.printStackTrace();
			
		}
	}
}

