package com.mangocity.data.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.util.HDFSUtil;
import com.mangocity.data.hadoop.util.HadoopConstants;
import com.mangocity.data.hadoop.util.HiveUtil;


/**
 *
 * @author shilei
 * @date 2016年4月26日 下午3:53:22 
 */
public class ArvoFileTransRunner extends Configured implements Tool{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ArvoFileTransRunner.class);

	private Parser parser = new Schema.Parser();
	
    private Configuration conf = HDFSUtil.getInstance().getConf();
    
    private String outputDir = PropertiesUtil.getValue(HadoopConstants.MAPREDUCE_OUTPUT_DIR, HadoopConstants.MAPREDUCE_OUTPUT_DIR_VALUE);
    
    private static boolean executeFlag = true;
    
    public ArvoFileTransRunner(){
    	PropertiesUtil.loadConfigFile("conf/mango-hadoop.properties");
    }
   
	@Override
	public Configuration getConf() {
		conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
		conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		Set<String> allowTableSet = new HashSet<String>();
		if(args.length>0){
			String allowTablesStr = args[args.length-1];
			if(StringUtils.startsWith(allowTablesStr, "allow_tables=")){
				String[] temp = StringUtils.split(allowTablesStr, "=");
				if(temp.length==2){
					String[] allowTables = StringUtils.split(temp[1],",");
					CollectionUtils.addAll(allowTableSet, allowTables);
				}
			}
			
		}
		LOGGER.info("allowTableSet {}",allowTableSet);
		// 1.从HiveMetaStore里获取所有的表
		Map<String,Table> hiveTableMap = HiveUtil.getInstance().getHiveTableMap();
		if(MapUtils.isEmpty(hiveTableMap)){
			return result;
		}
		
		Table table = null;
		for(Map.Entry<String,Table> entry:hiveTableMap.entrySet()){
			table = entry.getValue();
			String tableName = table.getTableName();
			//2. 处理table中的文件进行合并
			try {
				if(allowTableSet.size() == 0 || allowTableSet.contains(tableName)){
					result = handleTable(table);
				}
			} catch (Exception e) {
				LOGGER.error("execute table {} fialed", table.getTableName());
				result = 1;
			}
		}
		return result;
	}
	
	private int handleTable(Table table) throws IOException {
		
		
		int result = 0;
		String tableName = table.getTableName();
		String tableType = HiveUtil.getInstance().getTableType(table);
		if(StringUtils.isBlank(tableType)){
			LOGGER.error("table {} is not arvo file.", tableName);
			return result;
		}
		
		Map<String,List<Path>> inputPathMap = HiveUtil.getInstance().getInputPathMapByTable(table,tableType);
		if(MapUtils.isEmpty(inputPathMap)){
			return result;
		}
		Schema schema = HiveUtil.getInstance().getSchemaByTable(table);
		
		String mapKey = null;
		List<Path> pathList = null; 
		
		String reduceOutPath = null;
		List<Path> totalPath = new ArrayList<Path>();
		LOGGER.info("table name:{} , tableType:{}", tableName,tableType);
		//table.getParameters();
		for(Map.Entry<String,List<Path>> entry:inputPathMap.entrySet()){
			mapKey = entry.getKey();
			pathList = entry.getValue();
			if(pathList.size() >= 2 && (HadoopConstants.EXECUTE_BUCKET_TABLE.equals(tableType) || HadoopConstants.EXECUTE_PARTITION_BUCKET_TABLE.equals(tableType))){
				// 桶表
				reduceOutPath = StringUtils.join(mapKey,"_",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				result = submitJob(pathList, reduceOutPath,schema);
			}else if(HadoopConstants.EXECUTE_PARTITION_TABLE.equals(tableType)){
				// 分区表
				reduceOutPath = StringUtils.join(mapKey,"/",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				result = submitJob(pathList, reduceOutPath,schema);
			}else if(HadoopConstants.EXECUTE_TABLE.equals(tableType)){
				// 普通表
				reduceOutPath = StringUtils.join(mapKey,"/",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				totalPath.addAll(pathList);
			}
		}
		if(CollectionUtils.isNotEmpty(totalPath) && totalPath.size()>=2){//普通表的合并
			result = submitJob(totalPath, reduceOutPath,schema);
		}
		return result;
	}

	private int submitJob(List<Path> pathList, String reduceOutPath,Schema schema)
			throws IOException {
		int result = 0;
		Configuration jobConf;
		boolean flag;
		jobConf = new Configuration(conf);
		jobConf.set(HadoopConstants.REDUCE_OUT_PATH, reduceOutPath);
		
		Path outputPath = new Path(outputDir);
		Job job = createJob(jobConf,schema,outputPath);
		for (Path path : pathList) {
			LOGGER.info("input path {}", path);
			MultipleInputs.addInputPath(job, path, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
		}
		LOGGER.info("merge arvo file size:{}",pathList.size());
		LOGGER.info("reduceOutPath {}", reduceOutPath);
		try {
			result = (job.waitForCompletion(true) ? 0 : 1);
			flag = checkFile(reduceOutPath,outputPath);
			LOGGER.info("flag:{}",flag);
			if(flag){//合并成功
				for (Path path : pathList) {//删除合并前的文件
					HDFSUtil.getInstance().deleteFile(path);
				}
			}else{
				executeFlag = false;
			}
		} catch (Exception e) {
			executeFlag = false;
			LOGGER.error("check file fail.",e);
		}
		return result;
	}
	
	public Job createJob(Configuration conf,Schema schema,Path outputPath) throws IOException{
		Job job = Job.getInstance(conf);
        job.setJarByClass(ArvoFileTransRunner.class);
        job.setJobName(ArvoFileTransRunner.class.getName());
        if(System.getProperty("os.name").startsWith("Windows")){
        	job.setJar("E:\\shilei\\workspace\\mangocity-data\\mangocity-data-hadoop\\target\\mangocity-data-hadoop-1.0.0-jar-with-dependencies.jar");
        }
        
        job.setMapperClass(ArvoFileTransMapper.class);
        job.setReducerClass(ArvoFileTransReducer.class);
        
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);  
		AvroJob.setMapOutputValueSchema(job, schema);
		
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        
        AvroMultipleOutputs.addNamedOutput(job, HadoopConstants.NAMED_OUTPUT, AvroKeyOutputFormat.class, schema, null);
		AvroJob.setOutputKeySchema(job, schema);
		
		HDFSUtil.getInstance().deleteFile(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
	}

	/**
	 * 验证文件合并有没有成功
	 *
	 * @author shilei
	 * @date 2016年5月17日 下午5:19:34
	 * @param reduceOutPath
	 * @throws IOException 
	 */
	private boolean checkFile(String reduceOutPathStr,Path outputPath) throws IOException {
		boolean flag = false;
		Path successPath = new Path(outputPath, "_SUCCESS");
		if (!HDFSUtil.getInstance().getFileSystem().exists(successPath)) {
			LOGGER.error("处理失败，结果文件目录->{}",outputPath);
			return flag;
		}
		
		Path reduceOutPath = new Path(reduceOutPathStr);
		Path parentPath = reduceOutPath.getParent();
		
		String prefix = FilenameUtils.getBaseName(reduceOutPathStr);
		List<LocatedFileStatus> fileList = HDFSUtil.getInstance().readPathSubFile(parentPath);
		Path filePath = null;
		String fileName = null;
		
		for (LocatedFileStatus locatedFileStatus : fileList) {
			filePath = locatedFileStatus.getPath();
			fileName = filePath.getName();
			if(fileName.contains(prefix)){
				flag = true;
				break;
			}
		}
		return flag;
	}

	public static void main(String[] args) {
		try {
			if(System.getProperty("os.name").startsWith("Windows")){
	    		System.setProperty("HADOOP_USER_NAME", "hadoop");
	    	}
			new ArvoFileTransRunner().getConf();
//			args = new String[2];
//			args[0] = "hdfs://10.10.4.115:9000/user/hive/warehouse/ods.db/lz_user_option_action_log";
//			args[1] = "hdfs://10.10.4.115:9000/user/hive/warehouse/ods.db/lz_user_option_action_log/temp";
			int exitCode = ToolRunner.run(new ArvoFileTransRunner(), args);
			if(exitCode == 0 && executeFlag == false){
				System.exit(1);
			}else{
				System.exit(exitCode);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

