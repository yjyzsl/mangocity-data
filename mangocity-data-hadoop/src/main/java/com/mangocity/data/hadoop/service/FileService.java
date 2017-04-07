package com.mangocity.data.hadoop.service;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.Constants;
import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.util.HDFSUtil;
import com.mangocity.data.hadoop.util.HadoopConstants;

/**
 * 
 * @author shilei
 * @date 2016年5月11日 下午6:02:27 
 */
public class FileService {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(FileService.class);
	
	/**
	 * 将网站数据上传到hdfs
	 * 上传成功后则保存到临时文件夹
	 * @author shilei
	 * @date 2016年5月11日 下午6:06:32
	 * @return
	 */
	public boolean uploadFileFromWeb(){
		boolean flag = false;
		String filePath = PropertiesUtil.getDataFilePath()+File.separator+Constants.DATA_SERVICE_TYPE_WEB;
		String fileTempPath =  filePath+File.separator+HadoopConstants.DATA_SERVICE_TYPE_TEMP;
		File fileDir = new File(filePath);
		Collection<File> files = FileUtils.listFiles(fileDir, FileFilterUtils.suffixFileFilter(HadoopConstants.AVRO_FILE_TAG), null);
		String fileName = null;
		String hdfsPath = null;
		LOGGER.info("upload filePath {},file size:{}",filePath,files.size());
		for (File file : files) {
			try {
				fileName = fileDir + File.separator + file.getName();
				hdfsPath = HadoopConstants.HDFS_WEB_DATA_PATH  + File.separator +file.getName();
				LOGGER.info("upload file {} ",fileName);
				flag = HDFSUtil.getInstance().uploadFile(fileName, hdfsPath);
				if(flag){// 上传成功后则将文件移动到临时文件夹
					LOGGER.info("upload file {} success.",fileName);
					moveToDirectory(file,fileTempPath);
				}
			} catch (IOException e) {
				LOGGER.error("upload File to hdfs fial,file name : {} ",file.getAbsolutePath(),e);
			}
		}
		return flag;
	}
	//E:\shilei\work\项目资料\dsp\数据备份20160710010703074-20160711010310561
	public boolean uploadFile(){
		boolean flag = false;
		String filePath = "E:\\test";
		File fileDir = new File(filePath);
		Collection<File> files = FileUtils.listFiles(fileDir, FileFilterUtils.suffixFileFilter(HadoopConstants.AVRO_FILE_TAG), null);
		String fileName = null;
		String hdfsPath = null;
		LOGGER.info("upload filePath {},file size:{}",filePath,files.size());
		for (File file : files) {
			try {
				fileName = fileDir + File.separator + file.getName();
				hdfsPath = HadoopConstants.HDFS_WEB_DATA_PATH  + File.separator +file.getName();
				LOGGER.info("upload file {} ",fileName);
				flag = HDFSUtil.getInstance().uploadFile(fileName, hdfsPath);
			} catch (IOException e) {
				LOGGER.error("upload File to hdfs fial,file name : {} ",file.getAbsolutePath(),e);
			}
		}
		return flag;
	}
	
	public static void main(String[] args) {
		new FileService().uploadFile();
	}
	
	/**
	 * 将文件移动到指定目录
	 *
	 * @author shilei
	 * @date 2016年5月12日 上午8:54:32
	 * @param srcFile
	 * @param fileTempPath
	 * @throws IOException
	 */
	private void moveToDirectory(File srcFile,String fileTempPath) throws IOException{
		String fileBaseName = FilenameUtils.getBaseName(srcFile.getName());
		// 取得日期天
		fileTempPath = fileTempPath + File.separator + StringUtils.substring(fileBaseName, 0, 8);
		File destDir = new File(fileTempPath);
		FileUtils.moveToDirectory(srcFile, destDir, true);
	}
	
	/**
	 * 
	 * 删除web的本地数据
	 * @author shilei
	 * @throws IOException 
	 * @date 2016年5月11日 下午8:10:08
	 */
	public void deleteFileFromWeb() throws IOException{
		String filePath = PropertiesUtil.getDataFilePath()+File.separator+Constants.DATA_SERVICE_TYPE_WEB;
		String fileTempPath =  filePath+File.separator+HadoopConstants.DATA_SERVICE_TYPE_TEMP;
		this.deleteFile(filePath,fileTempPath);
	}
	
	private void deleteFile(String filePath,String fileTempPath) throws IOException{
		// 1.删除临时目录文件
		File fileDir = new File(fileTempPath);
		File[] files = fileDir.listFiles();
		
		Long deleteFileInterval = PropertiesUtil.getNumberValue(HadoopConstants.DELETE_FILE_INTERVAL, HadoopConstants.DELETE_FILE_INTERVAL_VALUE)*1000L;
		for (File file : files) {
			Date date;
			try {
				date = DateUtils.parseDate(FilenameUtils.getBaseName(file.getPath()), HadoopConstants.DATE_FORMAT_PATTERN_8);
				Long fileTime = date.getTime();
				Long interval = System.currentTimeMillis() - fileTime;
				if(interval > deleteFileInterval){
					LOGGER.info("delete file {}.",file.getAbsoluteFile());
					FileUtils.deleteQuietly(file);
				}
			} catch (ParseException e) {
				LOGGER.error("delete file fial,file name : {} ",file.getAbsolutePath(),e);
			}
		}
		// 2.删除主目录里超过时间段的文件
		fileDir = new File(filePath);
		files = fileDir.listFiles();
		String fileBaseName = null;
		
		for (File file : files) {
			if(file.isDirectory()){
				continue;
			}
			fileBaseName = FilenameUtils.getBaseName(file.getPath());
			Date date;
			try {
				date = DateUtils.parseDate(fileBaseName, HadoopConstants.DATE_FORMAT_PATTERN_17);
				Long fileTime = date.getTime();
				Long interval = System.currentTimeMillis() - fileTime;
				if(interval > deleteFileInterval){
					file.delete();
					LOGGER.info("delete file {}.",file.getAbsoluteFile());
				}
			} catch (ParseException e) {
				LOGGER.error("delete file fial,file name : {} ",file.getAbsolutePath(),e);
			}
		}
	}
	
}

