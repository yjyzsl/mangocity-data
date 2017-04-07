package com.mangocity.data.hadoop.main;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.service.FileService;
import com.mangocity.data.hadoop.util.HadoopConstants;

/**
 * 处理本地文件上传到HDFS上的主类
 * @author shilei
 * @date 2016年5月12日 下午4:56:37 
 */
public class FileMain {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(FileMain.class);
	
	public static void main(String[] args) {
		// 初始化
		init();
		
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
		// 启动文件处理线程
		final FileService fileService = new FileService();
		FileHandlerRunnable fileHandlerRunnable = new FileHandlerRunnable(fileService);
		int delay = PropertiesUtil.getNumberValue(HadoopConstants.UPLOAD_FILE_INTERVAL, HadoopConstants.UPLOAD_FILE_INTERVAL_VALUE);
		scheduledExecutorService.scheduleWithFixedDelay(fileHandlerRunnable, 0, delay, TimeUnit.SECONDS);
		
	}
	
	public static void init(){
		PropertiesUtil.loadConfigFile("conf/mango-hadoop.properties");
	}
	
	static class FileHandlerRunnable implements Runnable{
		
		private FileService fileService;
		
		public FileHandlerRunnable(FileService fileService) {
			super();
			this.fileService = fileService;
		}

		public void run() {
 			try {
 				// 上传文件
 	 			fileService.uploadFileFromWeb();
 	 			// 删除文件
				fileService.deleteFileFromWeb();
			} catch (IOException e) {
				LOGGER.error("upload file or delete file fial.",e);
			}
		}
	}

}

