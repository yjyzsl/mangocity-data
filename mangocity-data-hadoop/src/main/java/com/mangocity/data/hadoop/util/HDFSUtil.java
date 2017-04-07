package com.mangocity.data.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author shilei
 * @date 2016年5月10日 下午5:49:12 
 */
public class HDFSUtil {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(HDFSUtil.class);
	
	private Configuration configuration;
	
	private FileSystem fs;
	
	private static HDFSUtil hdfsUtil;
	
	private HDFSUtil(){
		configuration = new Configuration();
		InputStream inputStream = null;
		try {
			LOGGER.info(HDFSUtil.class.getClassLoader().getResource("conf/hadoop_pro/core-site.xml").getPath());
			inputStream = HDFSUtil.class.getClassLoader().getResourceAsStream("conf/hadoop_pro/core-site.xml");
			configuration.addResource(inputStream);
			
			inputStream = HDFSUtil.class.getClassLoader().getResourceAsStream("conf/hadoop_pro/hdfs-site.xml");
			configuration.addResource(inputStream);
			
			inputStream = HDFSUtil.class.getClassLoader().getResourceAsStream("conf/hadoop_pro/mapred-site.xml");
			configuration.addResource(inputStream);
			
			inputStream = HDFSUtil.class.getClassLoader().getResourceAsStream("conf/hadoop_pro/yarn-site.xml");
			
			configuration.addResource(inputStream);
		} catch (Exception e) {
			LOGGER.error("load hadoop configuration fial", e);	
		}
	}
	
	public static HDFSUtil getInstance(){
		if(hdfsUtil == null){
			synchronized (HDFSUtil.class) {
				if(hdfsUtil!=null) {
					return hdfsUtil;
				}
				hdfsUtil = new HDFSUtil();
			}
		}
		return hdfsUtil;
	}
	
	public Configuration getConf(){
		return configuration;
	}
	
	public String getValue(String keyName){
		return configuration.get(keyName);
	}
	
	public FileSystem getFileSystem(){
		try {
			if(fs == null){
				fs = FileSystem.get(configuration);
			}
		} catch (IOException e) {
			LOGGER.error("get FileSystem error", e);
		}
		return fs;
	}

    public boolean uploadFile(String fileName,String path) throws IOException{
    	File file = new File(fileName);
    	if(!file.exists()){
    		return false;
    	}
    	Path hdfsPath = new Path(path);
    	return uploadFile(file,hdfsPath);
    }
    
    public boolean uploadFile(File file,Path path) throws IOException{
    	InputStream fin = new FileInputStream(file);
    	return uploadFile(fin,path);
    }
    
    public boolean uploadFile(InputStream fin,Path path) throws IOException{
    	boolean flag = false;
    	FSDataOutputStream fos = null;
    	try {
    		int fileSize = fin.available();
    		if(fileSize == 0){
        		LOGGER.error("file name {} size is 0.", path);
        		return false;
        	}
    		flag = getFileSystem().exists(path);//存在则不需上传了
    		if(!flag){
    			fos = getFileSystem().create(path);
        		IOUtils.copyBytes(fin, fos, configuration);
        		flag = true;
    		}
		} finally{
			IOUtils.closeStream(fin);
			IOUtils.closeStream(fos);
		}
    	return flag;
    }
 
    public InputStream downloadFile(String path) throws IOException {
        Path hdfsPath =new Path(path);
        FSDataInputStream out = getFileSystem().open(hdfsPath);
        return out;
    }
    
    public boolean deleteFile(String pathStr) throws IOException {
    	Path path = new Path(pathStr);
        return deleteFile(path);  
    }
	
    public boolean deleteFile(Path path) throws IOException {
        boolean flag = getFileSystem().exists(path);
        if(flag){
        	flag = getFileSystem().delete(path, true);
        }
        return flag;  
    }
	
    /**
     * 读取文件夹下的所有文件
     *
     * @author shilei
     * @date 2016年5月17日 下午1:42:59
     * @param srcPathDir
     * @return
     */
	public List<LocatedFileStatus> readPathSubFile(Path srcPathDir){
		List<LocatedFileStatus> fileList = null;
		try {
			RemoteIterator<LocatedFileStatus> remoteIterator = getFileSystem().listFiles(srcPathDir, true);
			LocatedFileStatus locatedFileStatus = null;
			fileList = new ArrayList<LocatedFileStatus>();
			while(remoteIterator.hasNext()){
				locatedFileStatus = remoteIterator.next();
				fileList.add(locatedFileStatus);
			}
			return fileList;
		} catch (FileNotFoundException e) {
			LOGGER.error("srcPath {} {}",srcPathDir,e);
		} catch (IOException e) {
			LOGGER.error("srcPath {} {}",srcPathDir,e);
		}
		return fileList;
	}
	
	public static void main(String[] args) {
		Path path= new Path("hdfs://10.10.4.115:9000/tmp/testdata/20160615160003918.avro");
		 List<LocatedFileStatus> list = HDFSUtil.getInstance().readPathSubFile(path);
		 for (LocatedFileStatus locatedFileStatus : list) {
			 System.out.println(locatedFileStatus);
			System.out.println(locatedFileStatus.getLen());
		}
	}
	
	public static void main2(String[] args) {
		//getInstance().readPathSubFile(new Path("/user/hive/warehouse/ods.db/lz_user_option_action_log"));
		FSDataInputStream fin = null;
		BufferedReader in = null;
		try {
			fin = getInstance().getFileSystem().open(new Path("hdfs://10.10.4.115:9000/test/testdata/data/part-r-00000.deflate"));
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));  
			String line = null;
			while ((line = in.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally{
			IOUtils.closeStream(in);
			IOUtils.closeStream(fin);
		}
	}
}

