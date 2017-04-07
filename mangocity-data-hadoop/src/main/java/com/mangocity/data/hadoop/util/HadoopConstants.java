package com.mangocity.data.hadoop.util;

import com.mangocity.data.commons.util.Constants;

/**
 *
 * @author shilei
 * @date 2016年5月11日 下午7:39:06 
 */
public interface HadoopConstants extends Constants{

	/** 删除文件的时间间隔 以分钟为单位  默认1天 */
	int DELETE_FILE_INTERVAL_VALUE = 7*24*60*60;
	
	/** 文件上传完后保存的临时目录 */
	String DATA_SERVICE_TYPE_TEMP = "temp";
	
	/** 网站数据上传到hdfs上的路径  */
	String HDFS_WEB_DATA_PATH = "/dsp/tmp_datas/web"; 
	
	/**删除文件类型，校验：checkout，不校验：no_checkout*/
	String DELETE_FILE_TYPE = "delete_file_type";
	/**上传文件到HDFS的时间间隔,秒为单位*/
	int UPLOAD_FILE_INTERVAL_VALUE = 60;
	/**上传文件到HDFS的时间间隔,秒为单位*/
	String UPLOAD_FILE_INTERVAL = "upload_file_interval";
	/**删除文件时间间隔,分钟为单位,默认七天*/
	String DELETE_FILE_INTERVAL = "delete_file_interval";
	
	/** rudece输出目录 */
	String REDUCE_OUT_PATH = "reduce_out_path";
	
	String NAMED_OUTPUT = "mergeavrodata";
	
	String HIVE_METASTORE_URIS = "hive.metastore.uris";
	String HIVE_METASTORE_URIS_VALUE = "thrift://10.10.4.115:9083";
	String HIVE_METASTORE_LOCAL = "hive.metastore.local";
	String HIVE_METASTORE_LOCAL_VALUE = "false";
	
	String MAPREDUCE_OUTPUT_DIR = "mapreduce_output_dir";
	String MAPREDUCE_OUTPUT_DIR_VALUE = "/user/hive/mapreduceout/temp";
	
	String EXECUTE_TABLE = "EXECUTE_TABLE";
	String EXECUTE_PARTITION_TABLE = "EXECUTE_PARTITION_TABLE";
	String EXECUTE_BUCKET_TABLE = "EXECUTE_BUCKET_TABLE";
	String EXECUTE_PARTITION_BUCKET_TABLE = "EXECUTE_PARTITION_BUCKET_TABLE";
	
	String EXPECT_FS_VALUE = "hdfs://master.hadoop:9000";
	String EXPECT_FS = "expect_fs";
	
	String REPLACEMENT_FS = "fs.defaultFS";
	String REPLACEMENT_FS_VALUE = "hdfs://10.10.4.115:9000";
	
	String MERGE_TAG = "_merge";
	
	/** arvo格式的SerDe */
	String SERIALIZATION_LIB_ARVO = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
	
	String AVRO_SCHEMA_URL = "avro.schema.url";
	
	/**hdfs文件合时文件大小最大不能超过4个block的大小*/
	int HDFS_FILE_MAX_BLOCK_NUM_VALUE = 4;
	String HDFS_FILE_MAX_BLOCK_NUM = "hdfs_file_max_block_num";
	
	/**是否对hive表进行过滤*/
	String CHECK_TABLE = "check_table";
	boolean CHECK_TABLE_VALUE = true;
	
	/**不需要过滤的表*/
	String ALLOW_TABLES = "allow_tables";
	String ALLOW_TABLES_VALUE = "lz_user_option_action_log,tmp_user_option_action_log";
	
}

