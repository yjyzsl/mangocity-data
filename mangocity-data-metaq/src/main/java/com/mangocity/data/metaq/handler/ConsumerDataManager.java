package com.mangocity.data.metaq.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.metaq.MetaqConstants;

/**
 *
 * @author shilei
 * @param <T>
 * @date 2016年5月5日 下午4:12:38 
 */
public class ConsumerDataManager implements InitializingBean,DisposableBean{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ConsumerDataManager.class);
	
	// 存放消息数据的队列
	private BlockingQueue<JSONObject> cosumerDataQueue;

	private Map<String,ConsumerDataService> consumerDataServiceMap = new HashMap<String, ConsumerDataService>();
	
	private ThreadPoolExecutor taskRunner;
	
	private Thread dataHandler;
	
	private final Object lock = new Object();
	
	// 处理线程数量
	private int handlerThreadNum;
	
	public ConsumerDataManager(){
		int queueMaxSize = PropertiesUtil.getNumberValue(MetaqConstants.QUEUE_MAX_SIZE, MetaqConstants.QUEUE_MAX_SIZE_VALUE);
		cosumerDataQueue = new LinkedBlockingQueue<JSONObject>(queueMaxSize);
		
	}
	/**
	 * 启动处理线程
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:07:50
	 * @throws Exception
	 */
	public void start()throws Exception {
		taskRunner = (ThreadPoolExecutor)Executors.newFixedThreadPool(handlerThreadNum);
		dataHandler = new Thread(new DataHandler());
		// 启动处理线程
		dataHandler.start();
		LOGGER.info("data handler thread start success.");
	}
	
	/**
	 * 关闭资源
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:07:25
	 * @throws Exception
	 */
	public void stop() throws Exception {
	    if (dataHandler != null) {
	    	LOGGER.info("data handler thread stop.");
	    	dataHandler.interrupt();
	    }
	    if (taskRunner != null) {
	    	LOGGER.info("task runner stop.");
	    	taskRunner.shutdownNow();
	    }
	    // 清空队列中数据
	    cosumerDataQueue.clear();
	}
	
	public void putCosumerData(JSONObject jsonObject) throws InterruptedException{
		cosumerDataQueue.put(jsonObject);
	}
	
	
	/**
	 * 处理数据
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:01:49
	 * @version ConsumerDataManager
	 */
	private class DataHandler implements Runnable{
		

		@Override
		public void run() {
			int takeSizeWait = PropertiesUtil.getNumberValue(MetaqConstants.TAKE_SIZE_WAIT,MetaqConstants.TAKE_SIZE_WAIT_VALUE);
			//接收存放一分钟的数据
			List<JSONObject> datas = new ArrayList<JSONObject>();
			Long minuteTime = System.currentTimeMillis();
			//时间间隔
			Long interval = 0L;
			//每次写文件的时间间隔
			int writerFileInterval = PropertiesUtil.getNumberValue(MetaqConstants.WRITER_FILE_INTERVAL,MetaqConstants.WRITER_FILE_INTERVAL_VALUE)*60*1000;
			Long timeout = 0L;
			int threadNum = -1;
			Long currentTime = 0L;
			JSONObject data = null;
			while (true) {
				try {
//					threadNum = ConsumerDataConstants.CONSUMER_DATA_LIST.size();
					threadNum = taskRunner.getActiveCount();
					if(threadNum >= handlerThreadNum){// 线程池里正在运行的线程大于等于线程池大小则不往下运行
//						LOGGER.info("threadNum {} - {}",threadNum,taskRunner);
						Thread.sleep(500);
						continue;
					}
					currentTime = System.currentTimeMillis();
					interval = currentTime - minuteTime;
					// 从队列里取消息的超时时间
					timeout = (writerFileInterval - interval) < 0 ? 0 : (writerFileInterval - interval);
					// 从队列中取出一条json数据
					data = cosumerDataQueue.poll(timeout, TimeUnit.MILLISECONDS);
					if(interval >= writerFileInterval || datas.size() >= takeSizeWait){//达到了写数据的时间间隔或队列指定容量
						LOGGER.info("currentTime {} minuteTime {} -- {} -- writerFileInterval:{} , timeout {} ,datas size:{} , threadNum {}.",currentTime,minuteTime,interval,writerFileInterval,timeout,datas.size(),threadNum);
						if(CollectionUtils.isNotEmpty(datas)){//说明上一分钟的数据已经收集完成
							
							// 创建CosumerData对象  minuteTime 12:10 currentTime 12:12 14:10
							CosumerData cosumerData = generateCosumerData(datas);
							
							taskRunner.submit(cosumerData);
							LOGGER.info("taskRunner submit cosumerData size {}",datas.size());
							
							ConsumerDataConstants.CONSUMER_DATA_LIST.add(cosumerData);
							
							datas.clear();
							datas = new ArrayList<JSONObject>();
						}
						if(data!=null){
							datas.add(data);
						}
						// 设置到下一个文件的时间
						minuteTime = currentTime;
					}else if(data!=null){
						datas.add(data);
					}
				} catch (InterruptedException e) {
					LOGGER.error("Returning, interrupted : " + e);
			        break;
				}
			}
		}
		

		/**
		 * 创建CosumerData对象
		 *
		 * @author shilei
		 * @date 2016年5月6日 上午9:16:53
		 * @param userOperaTionLogBean
		 * @return
		 */
		private CosumerData generateCosumerData(List<JSONObject> datas) {
			CosumerData cosumerData = new CosumerData(consumerDataServiceMap);
			List<JSONObject> tempDatas = new ArrayList<JSONObject>();
			tempDatas.addAll(datas);
			//接收一分钟的数据
			cosumerData.setDatas(tempDatas);
			//精确到分钟的时间
			Long minuteTime = NumberUtils.toLong(DateFormatUtils.format(System.currentTimeMillis(), MetaqConstants.DATE_FORMAT_PATTERN_17));
			String fileName = minuteTime + MetaqConstants.AVRO_FILE_TAG;
			cosumerData.setFileName(fileName);
			
			String fileDir = PropertiesUtil.getDataFilePath();
			cosumerData.setFileDir(fileDir);
			
			return cosumerData;
		}
	}

	@Override
	public void destroy() throws Exception {
		this.stop();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		PropertiesUtil.loadConfigFile("conf/metaq.properties");
		PropertiesUtil.loadConfigFile("conf/config.properties");
		this.start();
	}

	@SuppressWarnings("unchecked")
	public void setConsumerDataServiceMap(Map consumerDataServiceMap) {
		this.consumerDataServiceMap = consumerDataServiceMap;
	}

	public int getHandlerThreadNum() {
		return handlerThreadNum;
	}

	public void setHandlerThreadNum(int handlerThreadNum) {
		this.handlerThreadNum = handlerThreadNum;
	}
	
}

