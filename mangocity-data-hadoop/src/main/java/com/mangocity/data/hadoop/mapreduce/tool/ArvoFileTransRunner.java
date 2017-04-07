package com.mangocity.data.hadoop.mapreduce.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
		int result = 1;
		String tableName = table.getTableName();
		String tableType = HiveUtil.getInstance().getTableType(table);
		if(!"lz_user_option_action_log".equals(tableName)){
			return result;
		}
		
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
		
		String reduceOutPath = "/user/hive/warehouse-backup/ods.db/lz_user_option_action_log_20160627/20160627";
		List<Path> totalPath = new ArrayList<Path>();
		
		//table.getParameters();
		for(Map.Entry<String,List<Path>> entry:inputPathMap.entrySet()){
			mapKey = entry.getKey();
			pathList = entry.getValue();
			totalPath.addAll(pathList);
		}
		if(CollectionUtils.isNotEmpty(totalPath) && totalPath.size()>=2){//普通表的合并
			result = submitJob(totalPath, reduceOutPath,schema);
		}
		return result;
	}

	private int submitJob(List<Path> pathList, String reduceOutPath,Schema schema)
			throws IOException {
		int result = 1;
		Configuration jobConf;
		jobConf = new Configuration(conf);
		jobConf.set(HadoopConstants.REDUCE_OUT_PATH, reduceOutPath);
		Job job = createJob(jobConf,schema);
		for (Path path : pathList) {
			LOGGER.info("input path {}", path);
			MultipleInputs.addInputPath(job, path, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
		}
//		String inputPath = HadoopConstants.REPLACEMENT_FS_VALUE+"/user/hive/warehouse-backup/ods.db/lz_user_option_action_log_20160612/20160612-r-00000.avro";
//		Path path = new Path(inputPath);
//		MultipleInputs.addInputPath(job, path, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
//		LOGGER.info("input path {}", path);
		LOGGER.info("merge arvo file size:{}",pathList.size());
		LOGGER.info("reduceOutPath {}", reduceOutPath);
		try {
			result = (job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			LOGGER.error("check file fail. {}",e);
		}
		return result;
	}
	
	public Job createJob(Configuration conf,Schema schema) throws IOException{
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
		
		String outputDir = PropertiesUtil.getValue(HadoopConstants.MAPREDUCE_OUTPUT_DIR, HadoopConstants.MAPREDUCE_OUTPUT_DIR_VALUE);
		Path outputPath = new Path(outputDir);
		HDFSUtil.getInstance().deleteFile(outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		return job;
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
			System.exit(exitCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

