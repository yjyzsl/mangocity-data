package com.mangocity.data.hadoop;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 *
 * @author shilei
 * @date 2016年5月18日 上午9:08:43 
 */
public class HiveCli {

	/**
	 *
	 * @author shilei
	 * @date 2016年5月18日 上午9:48:37
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		HiveConf conf = new HiveConf();
		conf.set("hive.metastore.uris", "thrift://10.10.4.115:9083");
		conf.set("hive.metastore.local", "false");
		HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
		List<String> allDatabases = client.getAllDatabases();
		for (String db : allDatabases) {
			List<String> allTables = client.getAllTables(db);
			System.out.println("db--->"+db);
			for (String tb : allTables) {
				
				//System.out.println("\t tb--->"+tb);
				Table table = client.getTable(db, tb);
				if(!("user_option_action".equals(table.getTableName()) || "tmp_user_option_action_log".equals(table.getTableName()))){
				//if(!"lz_user_option_action_log".equals(tb.toString())){
					continue;
				}
				System.out.println("\t tb--->"+tb);
				//System.out.println("\t\t table--->"+table);
				List<FieldSchema> partitionKeys = table.getPartitionKeys();
				
				System.out.println("\t\t parmarter--->"+table.getParameters());
				StorageDescriptor sd = table.getSd();
				int bucketColsSize = sd.getBucketColsSize();
				int partitionKeysSize = table.getPartitionKeysSize();
				String location = sd.getLocation();
				StringBuilder buff = new StringBuilder();
				buff.append("^").append(location).append("/.*");
				for (FieldSchema fieldSchema : partitionKeys) {
					buff.append(fieldSchema.getName()).append("=.*/");
				}
				buff.deleteCharAt(buff.length()-1);
				buff.append("$");
				Pattern	pattern = Pattern.compile(buff.toString());
				String str = location+"/user/hive/warehouse/ods.db/lz_user_option_action_log/operationyear=2016/operationmonth=5/operationday=15";
				Matcher matcher = pattern.matcher(str);
				boolean flag = matcher.matches();
				System.out.println("\t\t "+sd.getParameters());
				System.out.println("\t\t "+str);
				System.out.println("\t\t "+buff.toString());
				System.out.println("\t\t flag:"+flag);
				//System.out.println("\t\t sd:"+sd);
				System.out.println("\t\t buckets--->"+sd.getBucketCols());
				System.out.println("\t\t buckets-sorts--->"+sd.getSortCols());
				System.out.println("\t\t bucketColsSize:"+bucketColsSize+"  "+"partitionKeysSize:"+partitionKeysSize);
				//location:hdfs://master.hadoop:9000/user/hive/warehouse/ods.db/user_option_action_log_partition
//				System.out.println("\t\t tostring--->"+table.toString());
			}
		}
		
	}
}

