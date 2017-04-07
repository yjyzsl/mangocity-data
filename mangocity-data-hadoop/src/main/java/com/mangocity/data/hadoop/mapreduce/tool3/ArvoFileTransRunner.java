package com.mangocity.data.hadoop.mapreduce.tool3;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.util.HDFSUtil;
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
		Job job = Job.getInstance(conf);
        job.setJarByClass(ArvoFileTransRunner.class);
        job.setJobName(ArvoFileTransRunner.class.getName());
        if(System.getProperty("os.name").startsWith("Windows")){
        	job.setJar("E:\\shilei\\workspace\\mangocity-data\\mangocity-data-hadoop\\target\\mangocity-data-hadoop-1.0.0-jar-with-dependencies.jar");
        }
        
        job.setMapperClass(ArvoFileTransMapper.class);
        job.setCombinerClass(ArvoFileTransReducer.class);
        job.setReducerClass(ArvoFileTransReducer.class);
        
        String avroSchemaUrl = args[0];
        LOGGER.info("avroSchemaUrl {} ",avroSchemaUrl);
        String avroSchemaStr = VfsUtils.readFileToString(avroSchemaUrl);
		//LOGGER.info("avroSchemaUrl {} , avroSchemaStr {}",avroSchemaUrl,avroSchemaStr);
		Schema schema = parser.parse(avroSchemaStr);
	        
        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroJob.setInputKeySchema(job, schema);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);
//		AvroJob.setMapOutputValueSchema(job, schema);
		AvroJob.setMapOutputKeySchema(job, schema);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);
        
        //job.setn
        Path path = new Path(args[0]);
		
		for(int i=1;i<args.length-1;i++){
			 path = new Path(args[i]);
			 System.out.println(path);
			 MultipleInputs.addInputPath(job, path, AvroKeyInputFormat.class, ArvoFileTransMapper.class);
		}
		Path outpath = new Path(args[args.length-1]);
        
		AvroJob.setOutputKeySchema(job, schema);
		FileOutputFormat.setOutputPath(job, outpath);
		
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

