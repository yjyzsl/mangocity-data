package com.mangocity.data.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import com.mangocity.data.hadoop.util.HDFSUtil;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
	private String classPath = AppTest.class.getClassLoader().getResource("").getPath();
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
//    public void testApp()
//    {
//        String pathStr = "hdfs://10.10.4.115:9000/user/hive/warehouse-backup/ods.db/lz_user_option_action_log_20160612/20160612-r-00000.avro";
//        Path path = new Path(pathStr);
//        Configuration conf = new Configuration();
//		SeekableInput seekableInput;
//		try {
//			seekableInput = new FsInput(path, conf);
//			List<LzUserOperaTionLogBean> list = com.mangocity.data.commons.util.AvroUtil.readAvroFileToBean(seekableInput, LzUserOperaTionLogBean.class);
//			Set<Long> set = new HashSet<Long>();
//			for (LzUserOperaTionLogBean userOperaTionLogBean : list) {
//				long sourcerowid = userOperaTionLogBean.getSourcerowid();
////				if(set.contains(sourcerowid)){
////					System.out.println(userOperaTionLogBean);
////				}
//				if(20160606223526605L == sourcerowid || sourcerowid==20160607071929392L){
//					System.out.println(userOperaTionLogBean);
//				}
//			}
//			System.out.println(list.size());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//        
//    }
    
    
//    public void testApp2()
//    {
//        String pathStr = "hdfs://10.10.4.115:9000/dsp/tmp_datas/web/20160615172559509.avro";
//        Path path = new Path(pathStr);
//        Configuration conf = new Configuration();
//		SeekableInput seekableInput;
//		try {
//			seekableInput = new FsInput(path, conf);
//			List<UserOperaTionLogBean> list = com.mangocity.data.commons.util.AvroUtil.readAvroFileToBean(seekableInput, UserOperaTionLogBean.class);
//			Set<Long> set = new HashSet<Long>();
//			for (UserOperaTionLogBean userOperaTionLogBean : list) {
////				if(userOperaTionLogBean.getRefer() == null){
////					continue;
////				}
////				Utf8 utf8 = (Utf8)userOperaTionLogBean.getRefer();
////				String refer = utf8.toString();
////				if(refer.contains("sid")){
////					System.out.println(userOperaTionLogBean.getRefer()+" "+userOperaTionLogBean.getSid()+" "+userOperaTionLogBean.getProjectid());
////				}
////				if(userOperaTionLogBean.getOperationtype() !=null && userOperaTionLogBean.getOperationtype()==4){
////					
////				}
//				System.out.println(userOperaTionLogBean);
//			}
//			System.out.println(list.size());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//        
//    }
    
    public void testApp3()
    {
        String path = "C:\\Users\\shilei.MANGO\\data\\web\\20160615091126819.avro";
        System.out.println("classPath:"+classPath);
		try(InputStream schemaIn = this.getClass().getClassLoader().getResourceAsStream("user_option_action_log.avsc")){
			//-- max:20160710010503233 min:20160620080033843 --20160711010510570
			File file = new File(classPath, "20160710010503072.avro");
			Schema schema = new Schema.Parser().parse(schemaIn);
			List<GenericRecord> list = com.mangocity.data.commons.util.AvroUtil.readAvroFileToRecord(file, schema);
			for (GenericRecord genericRecord : list) {
				Long  sourcerowid = (Long)genericRecord.get("sourcerowid");
				System.out.println(sourcerowid);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
        
    }
    
    
    public void testApp4()
    {//8658 51567
        String pathStr = "hdfs://10.10.4.115:9000/dsp/tmp_datas/app_hive/productdetail/";
        Path path = new Path(pathStr);
        Configuration conf = new Configuration();
		SeekableInput seekableInput;
		try {//226463
			FileSystem fs = HDFSUtil.getInstance().getFileSystem();
//			RemoteIterator<LocatedFileStatus> outputFiles = fs.listLocatedStatus(path);
			List<LocatedFileStatus> list = HDFSUtil.getInstance().readPathSubFile(path);
			Long count = 0L;
			for (LocatedFileStatus locatedFileStatus : list) {
				seekableInput = new FsInput(locatedFileStatus.getPath(), conf);
				Schema.Parser parser = new Schema.Parser();
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("product_detail.avsc");
				Schema schema = parser.parse(in);
				
				List<GenericRecord> datas =  com.mangocity.data.commons.util.AvroUtil.readAvroFileToRecord(seekableInput, schema);
//				for (GenericRecord genericRecord : list) {
//					System.out.println(genericRecord);
//				}
				System.out.println(datas.size());
				count +=datas.size();
			}
			System.out.println(count);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
        
    }
    
    public void test(){
    	System.out.println(new Date(1464312712156L));
    }
}
