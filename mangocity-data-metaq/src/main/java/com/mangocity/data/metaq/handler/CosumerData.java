package com.mangocity.data.metaq.handler;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.metaq.MetaqConstants;

/**
 *
 * @author shilei
 * @date 2016年5月5日 下午6:06:42 
 */
public class CosumerData implements Runnable {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(CosumerData.class);
	
	// 数据文件存放目录
	private String fileDir;
	// 数据存放文件
	private String fileName;
    // 数据存放集合
	private List<JSONObject> datas;
	
	private AtomicInteger writerCount = new AtomicInteger(0);
	
	private ConsumerDataManager consumerDataManager;
	
	private Map<String,ConsumerDataService> consumerDataServiceMap;
	
	public CosumerData(Map<String,ConsumerDataService> consumerDataServiceMap) {
		super();
		this.consumerDataServiceMap = consumerDataServiceMap;
	}

	public CosumerData() {
	}


	@SuppressWarnings("rawtypes")
	@Override
	public void run() {
		String threadName = Thread.currentThread().getName();
		try {
			LOGGER.info("thread name : {} cosumerdata running , datas size {}",threadName,datas.size());
			if(CollectionUtils.isEmpty(datas)){
				LOGGER.warn("thread name : {} cosumer data is empty",threadName);
				return ;
			}
			ConsumerDataService consumerDataService = null;
			String dataServiceType = null;
			Object bean = null;
			Map<String,List<Object>> dataMap = new HashMap<String,List<Object>>();
			for (JSONObject data : datas) {
				try{
					dataServiceType = data.getString(MetaqConstants.DATA_SERVICE_TYPE_FIELD);
					if(StringUtils.isBlank(dataServiceType)){
						dataServiceType = MetaqConstants.DATA_SERVICE_TYPE_WEB;
					}
					consumerDataService = consumerDataServiceMap.get(dataServiceType);
					// json转化成相应的bean
				
					bean = consumerDataService.transformUOTL4JSON(data);
					
					if(bean==null){//对象为空表示出现重复
						continue;
					}
					List<Object> consumerDatas = dataMap.get(dataServiceType);
					if(consumerDatas==null){
						consumerDatas = new ArrayList<Object>();
						dataMap.put(dataServiceType, consumerDatas);
					}
					consumerDatas.add(bean);
				} catch (Exception e) {
					LOGGER.error("transfrom fail : {}",data,e);
				}
			}
			// 将dataMap中的数据全部写入到file文件中
			writerDatas(dataMap);
		}catch(Exception e){
			LOGGER.error("{}",e);
		}finally{
			// 清空集合中的对象
			this.clear();
		}
	}
	
	
	
	private void writerDatas(Map<String, List<Object>> dataMap) {
		String dataServiceType = null;
		List<Object> consumerDatas = null;
		ConsumerDataService consumerDataService = null;
		for(Map.Entry<String, List<Object>> entry :dataMap.entrySet()){
			dataServiceType = entry.getKey();
			consumerDatas = entry.getValue();
			consumerDataService = consumerDataServiceMap.get(dataServiceType);
			
			fileDir = fileDir + File.separator + dataServiceType;
			File file = new File(fileDir);
			if(!file.exists()){
				file.mkdirs();
			}
			fileName = fileDir + File.separator + fileName; 
			file = new File(fileName);
			if(!file.exists()){
				try {
					file.createNewFile();
				} catch (IOException e) {
					LOGGER.error("createNewFile error fileName {}", fileName,e);
				}
			}
			writerDatas(file,consumerDatas,consumerDataService);
		}
		
		
	}

	/**
	 * 将datas中的数据全部写入到file文件中
	 *
	 * @author shilei
	 * @date 2016年5月9日 下午2:46:53
	 * @param file
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean writerDatas(File file,List<Object> consumerDatas,ConsumerDataService consumerDataService){
		String threadName = Thread.currentThread().getName();
		// 将datas中的数据全部写入到file文件中
		boolean writerFlag = consumerDataService.writerBeanToAvroFile(consumerDatas, file);
		// 重复写入次数
		int count = writerCount.incrementAndGet();
		if(writerFlag){//写入成功
			LOGGER.info("writerFlag : {} , threadName : {} , fileName : {} writer datas success , datas size : {} , consumerDatas size:{}",writerFlag,threadName,fileName,datas.size(),consumerDatas.size());
			// 方便垃圾回收
			consumerDatas.clear();
			consumerDatas = null;
			return writerFlag;
		}else{
			// TODO 没有写入成功处理操作
			LOGGER.error("writerCount ：{} fileName : {} writer datas error , datas size : {}", count,fileName,datas.size());
			if(count<=MetaqConstants.WRITER_DATA_COUNT){//已经超过最大次数了
				writerFlag = writerDatas(file,consumerDatas,consumerDataService);
			}else{
				consumerDatas.clear();
				consumerDatas = null;
			}
		}
		return writerFlag;
	}
	
	public void clear(){
		
		if(CollectionUtils.isNotEmpty(datas)){
			datas.clear();
			datas = null;
		}
		if(ConsumerDataConstants.CONSUMER_DATA_LIST.contains(this)){
			ConsumerDataConstants.CONSUMER_DATA_LIST.remove(this);
		}
	}

	public String getFileDir() {
		return fileDir;
	}

	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public List<JSONObject> getDatas() {
		return datas;
	}

	public void setDatas(List<JSONObject> datas) {
		this.datas = datas;
	}

	public void setConsumerDataServiceMap(Map<String, ConsumerDataService> consumerDataServiceMap) {
		this.consumerDataServiceMap = consumerDataServiceMap;
	}



	

	

	
	

}

