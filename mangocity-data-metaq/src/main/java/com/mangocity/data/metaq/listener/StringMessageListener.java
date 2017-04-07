package com.mangocity.data.metaq.listener;

import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.metaq.MetaqConstants;
import com.mangocity.data.metaq.handler.ConsumerDataManager;
import com.mangocity.data.metaq.handler.ParseMsg2Bean;
import com.taobao.metamorphosis.client.extension.spring.DefaultMessageListener;
import com.taobao.metamorphosis.client.extension.spring.MetaqMessage;
import com.taobao.metamorphosis.cluster.Partition;

public class StringMessageListener extends DefaultMessageListener<String> {

	private final static Logger LOGGER = LoggerFactory.getLogger(StringMessageListener.class);
	
	@Resource(name="consumerDataManager")
	private ConsumerDataManager consumerDataManager;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	@Override
	public void onReceiveMessages(MetaqMessage<String> msg) {
		if(msg == null){
			LOGGER.warn("metaq message is null.");
			return ;
		}
		count.incrementAndGet();
		try {
			// 1.将MetaqMessage转化成Json,消息中有很多重复的数据,将其转化成bean是会造成很多多余的数据
			JSONObject jsonObject = transformJson4Msg(msg);
			if(jsonObject!=null){
				jsonObject.put(MetaqConstants.DATA_SERVICE_TYPE_FIELD, MetaqConstants.DATA_SERVICE_TYPE_WEB);
				// 2.将消息数据放到队列中
				putData2Queue(jsonObject,msg);
			}
			LOGGER.debug(" receive message count {} ", count.get());
		} catch (Exception e) {
			LOGGER.error(" transformUOTL error , msg : {}", msg.getBody() ,e);
		}
	}
	
	/**
	 * 将消息转化成json
	 *
	 * @author shilei
	 * @date 2016年5月6日 上午9:03:52
	 * @param message
	 * @return
	 */
    private JSONObject transformJson4Msg(MetaqMessage<String> msg) {
    	String message = msg.getBody();
//		LOGGER.info("receive message : {}",message);
		// 消息转化成Json
		JSONObject jsonObject = ParseMsg2Bean.msg2Json(message);
		//LOGGER.debug("thread name {} -- {}",Thread.currentThread().getName(),jsonObject.toString());
		return jsonObject;
	}


    
    /**
	 * 将消息数据放到队列中
	 *
	 * @author shilei
	 * @date 2016年5月6日 上午9:16:43
	 * @param cosumerData
	 * @param msg
	 */
	private void putData2Queue(JSONObject jsonObject,MetaqMessage<String> msg) {
		try {
			consumerDataManager.putCosumerData(jsonObject);
		} catch (InterruptedException e) {
			long id = msg.getId();
			String topic = msg.getTopic();
			boolean rollbackonly = false;
			Partition partition = msg.getPartition();
			LOGGER.error("put cosumer data fial id : {} , topic : {} ,  rollbackonly : {} , partition : {}", id,topic,rollbackonly,partition);
			if(rollbackonly){
				// 是否回滚消息
				msg.setRollbackOnly();
			}
		}
	}

	public void setConsumerDataManager(ConsumerDataManager consumerDataManager) {
		this.consumerDataManager = consumerDataManager;
	}


}


