package com.mangocity.data.hadoop.maped;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMultipleOutputs;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.mapreduce.ArvoFileTransRunner;
import com.mangocity.data.hadoop.util.HDFSUtil;
import com.mangocity.data.hadoop.util.HadoopConstants;
import com.mangocity.data.hadoop.util.HiveUtil;


/**
 * 针对hive表中arvo数据文件进行合并
 *
 * @author shilei
 * @date 2016年6月29日 下午5:59:44
 * @version
 */
public class ArvoFileMergeDrive extends Configured implements Tool{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ArvoFileTransRunner.class);
	
	private static String BATCH_NO = null;
	
    private Configuration conf = HDFSUtil.getInstance().getConf();
    
    private String outputDir = PropertiesUtil.getValue(HadoopConstants.MAPREDUCE_OUTPUT_DIR, HadoopConstants.MAPREDUCE_OUTPUT_DIR_VALUE);
    
    public ArvoFileMergeDrive(){
    	PropertiesUtil.loadConfigFile("conf/mango-hadoop.properties");
    }
   
	@Override
	public Configuration getConf() {
		conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
		//conf.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC);
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		
		// 1.从HiveMetaStore里获取所有的表
		Map<String,Table> hiveTableMap = HiveUtil.getInstance().getHiveTableMap();
		if(MapUtils.isEmpty(hiveTableMap)){
			return result;
		}
		
		Table table = null;
		for(Map.Entry<String,Table> entry:hiveTableMap.entrySet()){
			table = entry.getValue();
			//2. 处理table中的文件进行合并
			try {
				result = handleTable(table);
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
		LOGGER.info("=========================tableName {} , tableType {}:",tableName,tableType);
		if(StringUtils.isBlank(tableType)){
			LOGGER.error("table {} is not arvo file.", tableName);
			return result;
		}
		Map<String,List<Path>> inputPathMap = HiveUtil.getInstance().getInputPathMapByTable(table,tableType);
		if(MapUtils.isEmpty(inputPathMap)){
			LOGGER.info("=========================inputPathMap isEmpty");
			return result;
		}
		Schema schema = HiveUtil.getInstance().getSchemaByTable(table);
		
		String mapKey = null;
		List<Path> pathList = null; 
		
		String reduceOutPath = null;
		List<Path> totalPath = new ArrayList<Path>();
		
		//table.getParameters();
		for(Map.Entry<String,List<Path>> entry:inputPathMap.entrySet()){
			mapKey = entry.getKey();
			pathList = entry.getValue();
			if(pathList.size() >= 2 && (HadoopConstants.EXECUTE_BUCKET_TABLE.equals(tableType) || HadoopConstants.EXECUTE_PARTITION_BUCKET_TABLE.equals(tableType))){
				// 桶表
				reduceOutPath = StringUtils.join(mapKey,"_",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				LOGGER.info("========================= reduceOutPath {}:",reduceOutPath);
				result = submitJob(pathList, reduceOutPath,schema);
			}else if(HadoopConstants.EXECUTE_PARTITION_TABLE.equals(tableType)){
				// 分区表
				//reduceOutPath = StringUtils.join(mapKey,"/",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				reduceOutPath = mapKey;
				result = submitJob(pathList, reduceOutPath,schema);
			}else if(HadoopConstants.EXECUTE_TABLE.equals(tableType)){
				// 普通表
				//reduceOutPath = StringUtils.join(mapKey,"/",DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17),HadoopConstants.MERGE_TAG);
				reduceOutPath = mapKey;
				totalPath.addAll(pathList);
			}
		}
		LOGGER.info("===========reduceOutPath:{}",reduceOutPath);
		if(CollectionUtils.isNotEmpty(totalPath) && totalPath.size()>=2){//普通表的合并
			result = submitJob(totalPath, reduceOutPath,schema);
		}
		return result;
	}

	private int submitJob(List<Path> pathList, String reduceOutPath,Schema schema)
			throws IOException {
		BATCH_NO = DateFormatUtils.format(System.currentTimeMillis(), HadoopConstants.DATE_FORMAT_PATTERN_17);
		int result = 0;
		Configuration configuration;
		boolean flag;
		configuration = new Configuration(conf);
		configuration.set(HadoopConstants.REDUCE_OUT_PATH, reduceOutPath);
		String outputPathStr = outputDir+"/"+schema.getName();
		Path outputPath = new Path(outputPathStr);
		JobConf jobConf = createJob(configuration,schema,outputPath);
		for (Path path : pathList) {
			LOGGER.info("input path {}", path);
			MultipleInputs.addInputPath(jobConf, path, AvroInputFormat.class, ArvoFileMergeMapper.class);
		}
		LOGGER.info("merge arvo file size:{}",pathList.size());
		LOGGER.info("reduceOutPath {}", reduceOutPath);
		try {
			RunningJob runningJob = JobClient.runJob(jobConf);
			LOGGER.info("runningJob isSuccessful {}", runningJob.isSuccessful());
			flag = runningJob.isSuccessful();
			if(flag){
				flag = processReulstFile(reduceOutPath,outputPath,runningJob.getID().getJtIdentifier());
			}
//			if(flag){//合并成功
//				for (Path path : pathList) {//删除合并前的文件
//					HDFSUtil.getInstance().deleteFile(path);
//				}
//			}
		} catch (Exception e) {
			LOGGER.error("check file fail. {}",e);
		}
		return result;
	}
	
	public JobConf createJob(Configuration conf,Schema schema,Path outputPath) throws IOException{
		
		JobConf jobConf = new JobConf(conf, this.getClass());
		jobConf.setJobName(this.getClass().getName());
		if(System.getProperty("os.name").startsWith("Windows")){
			jobConf.setJar("E:\\shilei\\workspace\\hadoop\\target\\hadoop-1.0.0.jar");
		}
		
        jobConf.setMapperClass(ArvoFileMergeMapper.class);
        jobConf.setReducerClass(ArvoFileMergeReducer.class);
        
        jobConf.setMapOutputKeyClass(AvroKey.class);
        jobConf.setMapOutputValueClass(AvroValue.class);
        
        jobConf.setInputFormat(AvroInputFormat.class);
        jobConf.setOutputFormat(AvroOutputFormat.class);
        
        AvroJob.setInputSchema(jobConf, schema);
        AvroJob.setOutputSchema(jobConf, schema);
        AvroJob.setMapOutputSchema(jobConf, Pair.getPairSchema(Schema.create(Schema.Type.STRING),schema));
        String nameOutput = schema.getName();
        LOGGER.info("nameOutput:"+nameOutput);
        AvroMultipleOutputs.addMultiNamedOutput(jobConf, nameOutput, AvroOutputFormat.class, schema);
        
        HDFSUtil.getInstance().deleteFile(outputPath);
	    FileOutputFormat.setOutputPath(jobConf, outputPath);
	    jobConf.setNumReduceTasks(1);
		
		return jobConf;
	}

	/**
	 * 验证文件合并有没有成功
	 *
	 * @author shilei
	 * @date 2016年5月17日 下午5:19:34
	 * @param reduceOutPath
	 * @throws IOException 
	 */
	public static boolean processReulstFile(String reduceOutPathStr,Path outputPath, String jobId) throws IOException {
		boolean flag = false;
		Path successPath = new Path(outputPath, "_SUCCESS");
		FileSystem fs = HDFSUtil.getInstance().getFileSystem();
		if (!fs.exists(successPath)) {
			LOGGER.error("处理失败，结果文件目录->{}",outputPath);
			return flag;
		}
		RemoteIterator<LocatedFileStatus> outputFiles = fs.listLocatedStatus(outputPath);
		if (null != outputFiles) {
			while (outputFiles.hasNext()) {
				LocatedFileStatus outputFile = outputFiles.next();
				Path tmpPath = outputFile.getPath();
				if (!StringUtils.endsWithIgnoreCase(tmpPath.getName(), ".avro")) {
					continue;
				}
				if (StringUtils.isNotBlank(reduceOutPathStr)) {
					String newFileName = BATCH_NO+"-"+jobId+"-"+tmpPath.getName();
					Path destPath = new Path(reduceOutPathStr, newFileName);
					LOGGER.info("tmpPath:"+tmpPath);
					LOGGER.info("destPath:"+destPath);
					//fs.rename(tmpPath, destPath);
				} 
			}
		}
		flag = true;
		return flag;
	}
	
	public static void test(){
		Table table = null;
		try {
			//lz_user_option_action_log,
			table = HiveUtil.getInstance().getHiveTable("ods", "tmp_app_click_log");
			System.out.println(HiveUtil.getInstance().getTableType(table));
			Map<String,List<Path>> inputPathMap = HiveUtil.getInstance().getInputPathMapByTable(table);
			for (String string : inputPathMap.keySet()) {
				List<Path> pathList = inputPathMap.get(string);
				System.out.println("key:"+string);
				for (Path path : pathList) {
					System.out.println("-->path:"+path);
				}
			}
		} catch (NoSuchObjectException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
			
		}
		
	}
	
	public static void test2(){
		String reduceOutPathStr="hdfs://10.10.4.115:9000/tmp/app_process_test/merge/";
		Path outputPath = new Path("hdfs://10.10.4.115:9000/tmp/app_process_test/result_20160623163749/");
		String jobId = "1234567";
		
		try {
			processReulstFile( reduceOutPathStr,outputPath,jobId);
		} catch (IOException e) {
			e.printStackTrace();
			
		}
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
			int exitCode = ToolRunner.run(new ArvoFileMergeDrive(), args);
			System.exit(exitCode);
			
//			test();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

