package com.mangocity.etl.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mangocity.etl.handle.HandleService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.util.PropertiesUtil;

public class Main {
	
	private final static Logger LOGGER = Logger.getLogger(Main.class);
	
	private static ApplicationContext context = null;
	
	static{
		context = new ClassPathXmlApplicationContext("classpath:spring-mybatis.xml");
	}
	
	public static void main(String[] args) {
		
		InputStream in = null;
		OutputStream out = null;
		try {
			File file = new File("./sourcerowid");
			if(!file.exists()){
				file.createNewFile();
			}
			in = new FileInputStream(file);
			byte[] b = new byte[in.available()];
			in.read(b, 0, in.available());
			String sourcerowidStr = new String(b);
			Integer sourcerowid = -1;
			if(NumberUtils.isDigits(sourcerowidStr)){
				sourcerowid = NumberUtils.toInt(sourcerowidStr);
			}
			LOGGER.info("sourcerowid:"+sourcerowid);
			@SuppressWarnings("rawtypes")
			HandleService handleService = (HandleService) context.getBean(PropertiesUtil.getValue(Constants.HANDLE_SERVICE_TYPE));
			Integer sourcerowidMax = handleService.handle(sourcerowid);
			LOGGER.info("sourcerowid_max:"+sourcerowidMax);
			if(sourcerowidMax>sourcerowid){
				b = sourcerowidMax.toString().getBytes();
				out = new FileOutputStream(file);
				out.write(b);
				out.flush();
			}
		} catch (Exception e) {
			LOGGER.error("start service fial",e);
			e.printStackTrace();
		} finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			if(out!=null){
				try {
					out.close();
				} catch (IOException e) {
				}
			}
		}
		
		/****************************java方式定时调度******************************************/
//		ScheduledExecutorService scheduledExecutorService = null;
//		try {
//			HandleService handleService = (HandleService) context.getBean("handleService");
//			
//			scheduledExecutorService = Executors.newScheduledThreadPool(1);
//			Integer delay = PropertiesUtil.getNumberValue(Constants.SCHEDULE_INTERVAL);
//			HandleTask handleTask = new HandleTask(handleService); 
//			// 启动定时任务
//			scheduledExecutorService.scheduleWithFixedDelay(handleTask,0,delay,TimeUnit.SECONDS);
//			LOGGER.warn("定时任务启动...");
//			
//		} catch (Exception e) {
//			if(scheduledExecutorService!=null){
//				scheduledExecutorService.shutdown();
//			}
//			LOGGER.error("定时任务关闭...\n"+e);
//			e.printStackTrace();
//		} 
		/****************************java方式定时调度******************************************/
		
		
	}
}
class HandleTask implements Runnable{

	private final static Logger LOGGER = Logger.getLogger(HandleTask.class);
	private HandleService handleService;
	private Integer handleCount = 0;
	
	public HandleTask(HandleService handleService){
		this.handleService = handleService;
	}
	
	@Override
	public void run() {
		
		InputStream in = null;
		OutputStream out = null;
		try {
			File file = new File("");
			in = new FileInputStream(file.getAbsolutePath()+"/sourcerowid");
			byte[] b = new byte[in.available()];
			in.read(b, 0, in.available());
			String sourcerowidStr = new String(b);
			Integer sourcerowid = -1;
			if(NumberUtils.isDigits(sourcerowidStr)){
				sourcerowid = NumberUtils.toInt(sourcerowidStr);
			}
			LOGGER.info("sourcerowid:"+sourcerowid);
			Integer sourcerowidMax = handleService.handle(sourcerowid);
			LOGGER.info("sourcerowid_max:"+sourcerowidMax);
			if(sourcerowidMax>sourcerowid){
				b = sourcerowidMax.toString().getBytes();
				out = new FileOutputStream(file.getAbsolutePath()+"/sourcerowid");
				out.write(b);
				out.flush();
			}
		} catch (Exception e) {
			LOGGER.error(e);
			e.printStackTrace();
		} finally{
			if(in!=null){
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			if(out!=null){
				try {
					out.close();
				} catch (IOException e) {
				}
			}
			handleCount++;
			LOGGER.info("execute count:"+handleCount);
		}
	}
}