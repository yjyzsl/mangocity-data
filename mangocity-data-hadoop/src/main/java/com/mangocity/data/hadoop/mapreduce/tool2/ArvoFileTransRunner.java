package com.mangocity.data.hadoop.mapreduce.tool2;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.util.HDFSUtil;
import com.mangocity.data.hadoop.util.HadoopConstants;
import com.mangocity.data.hadoop.util.HiveUtil;
import com.mangocity.data.hadoop.util.VfsUtils;


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
		return conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		Path path1 = new Path(args[0]);
		Path path2 = new Path(args[1]);
		Path outpath = new Path(args[2]);
		Job job = Job.getInstance(conf);
        job.setJarByClass(ArvoFileTransRunner.class);
        job.setJobName(ArvoFileTransRunner.class.getName());
        if(System.getProperty("os.name").startsWith("Windows")){
        	job.setJar("E:\\shilei\\workspace\\mangocity-data\\mangocity-data-hadoop\\target\\mangocity-data-hadoop-1.0.0-jar-with-dependencies.jar");
        }
        
        job.setMapperClass(ArvoFileTransMapper.class);
        job.setCombinerClass(ArvoFileTransReducer.class);
        job.setReducerClass(ArvoFileTransReducer.class);
        
        String avroSchemaUrl = "hdfs://10.10.4.115:9000/user/hive/schema/user_option_action_log.avsc";
        String avroSchemaStr = VfsUtils.readFileToString(avroSchemaUrl);
		LOGGER.info("avroSchemaUrl {} , avroSchemaStr {}",avroSchemaUrl,avroSchemaStr);
		Schema schema = parser.parse(avroSchemaStr);
        
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);  
        job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
        
		job.setOutputFormatClass(TextOutputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
        
		FileOutputFormat.setOutputPath(job, outpath);
		MultipleInputs.addInputPath(job, path1, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
		MultipleInputs.addInputPath(job, path2, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
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

