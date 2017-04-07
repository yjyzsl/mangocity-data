package com.mangocity.data.metaq;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.commons.util.XmlUtil;
import com.taobao.metamorphosis.client.extension.spring.MessageBuilder;
import com.taobao.metamorphosis.client.extension.spring.MetaqTemplate;
import com.taobao.metamorphosis.client.producer.SendResult;

/**
 *
 * @author shilei
 * @date 2016年5月9日 下午3:11:00 
 */
public class MetaqProduceMain {
	
	private final static Logger LOGGER = Logger.getLogger(MetaqProduceMain.class);
	
	private MetaqTemplate metaqTemplate;

	private String topic;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	public SendResult sendMessage(Object messageObj){
		if(messageObj == null){
			return new SendResult(false, null, 0, "message body is null");
		}
		SendResult sendResult = null;
		try {
			sendResult = metaqTemplate.send(MessageBuilder.withTopic(topic).withBody(messageObj));
		} catch (InterruptedException e) {
			sendResult = new SendResult(false, null, 0, "send message fial:"+e.getMessage());
		}
		return sendResult;
	}
	

	public MetaqTemplate getMetaqTemplate() {
		return metaqTemplate;
	}

	public void setMetaqTemplate(MetaqTemplate metaqTemplate) {
		this.metaqTemplate = metaqTemplate;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public AtomicInteger getCount() {
		return count;
	}
	
	@Override
	public String toString() {
		return "SpringProducer [metaqTemplate=" + metaqTemplate + ", topic="
				+ topic + ", count=" + count + "]";
	}


	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath:conf/metaq-producer-beans.xml");
		MetaqProduceMain springProducer = (MetaqProduceMain)context.getBean("producer");
		
		InputStream dataIn = MetaqProduceMain.class.getClassLoader().getResourceAsStream("mysqldata.xml");
//		InputStream dataIn = MetaqProduceMain.class.getClassLoader().getResourceAsStream("data.xml");
		List<Map<String,String>> datas = null;
		try {
			datas = XmlUtil.parseXml(dataIn);
		} catch (DocumentException e) {
			e.printStackTrace();
		}
		LOGGER.info(springProducer);
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);
		for(int i=0;i<1;i++){
			MyTask task = new MyTask(springProducer,datas);
//			scheduledExecutorService.scheduleWithFixedDelay(task, 0,3600, TimeUnit.SECONDS);
			new Thread(task).start();
		}
		
	}

	
	static class MyTask implements Runnable{
		
		//private final static Logger LOGGER = Logger.getLogger(Task.class);

		private MetaqProduceMain producer;
		private List<Map<String,String>> datas;
		
		public MyTask(MetaqProduceMain producer,List<Map<String,String>> datas){
			this.producer = producer;
			this.datas = datas;
		}
		
		public void run() {
			try {
				String threadName = Thread.currentThread().getName();
				LOGGER.info("thread name ["+threadName+"] runing ...");
				JSONObject jsonObject = null;
				String value = null;
				
				long startTime = System.currentTimeMillis();
				LOGGER.info("start time:"+DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss SSS"));
				
				int totalCount = PropertiesUtil.getNumberValue("totalNum");
				AtomicInteger count = producer.getCount();
				//int count = 0;
				long saveStartTime = 0;
				SendResult sendResult = null;
				
				for(Map<String,String> data:datas){
					String dataStr = data.get("data");
					jsonObject = JSONObject.parseObject(dataStr);
					jsonObject.put("channel", data.get("channel"));
					sendResult = producer.sendMessage(jsonObject.toJSONString());
					if(count.get()%100==0){
//						try {
//							Thread.sleep(60*1000);
//						} catch (InterruptedException e) {
//							e.printStackTrace();
//						}
					}
					LOGGER.info("count:"+count.incrementAndGet()+","+sendResult.getOffset()+":"+sendResult.isSuccess()+" use time:"+(System.currentTimeMillis()-saveStartTime));
				}
				long endTime = System.currentTimeMillis();
				LOGGER.info("end time:"+DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd hh:mm:ss SSS"));
				LOGGER.info("threadName ["+threadName+"] , producer totalCount:"+totalCount+" , success count:"+ count+", use time:"+(endTime-startTime));
			} finally{}
		}
	}
}

