package com.mangocity.data.metaq;

import com.mangocity.data.commons.util.Constants;

/**
 *
 * @author shilei
 * @date 2016年5月9日 上午9:11:42 
 */
public interface MetaqConstants extends Constants{
	
	/**
	 * 选取作为时间转化的字段（转化为 年 月 日 时 分）
	 */
	String OPERATIONDAYFIELD = "operation_day_field";
	
	String JSONKEY = "jsonKey";
	String COLUMNKEY = "columnKey";
	String OPERATION = "operation";
	String PATTERN = "pattern";
	
	/** 字符串转int */
	String TO_INT = "to_int";
	/** 字符串转long */
	String TO_LONG = "to_long";
	/** 日期字符串转long */
	String DATE_TO_LONG = "date_to_long";
	/** 字符串转URLDecoder解码 */
	String TO_DECODE = "to_decode";
	
	
	/** 写arvo数据到本地出现错误时最多重写的次数  */
	Integer WRITER_DATA_COUNT = 3;
	
	/** 从metaq读消息写数据的时间间隔，单位为分钟*/
	String WRITER_FILE_INTERVAL = "writer_file_interval";
	/** 默认值为一分钟 */
	Integer WRITER_FILE_INTERVAL_VALUE = 1;
	
	String DATA_SERVICE_TYPE_FIELD = "dataservicetype";
	
	String DATA_FIELD = "data";
	
	Integer QUEUE_MAX_SIZE_VALUE = 512;
	String  QUEUE_MAX_SIZE = "queue_max_size";
	
	Integer QUEUE_SIZE_WAIT_VALUE = 512;
	String QUEUE_SIZE_WAIT = "queue_size_wait";
	
	/** 毫秒 */
	Integer QUEUE_TIME_WAIT_VALUE = 10;
	String QUEUE_TIME_WAIT = "queue_time_wait";
	/**集合里超过一定数量 写一次 默认一万*/
	Integer TAKE_SIZE_WAIT_VALUE = 10000;
	String TAKE_SIZE_WAIT = "take_size_wait";
	
	/** 是否需要通过http请求获取标题 默认false*/
	boolean HTTP_PAGE_TILE_VALUE = false;
	String HTTP_PAGE_TILE = "http_page_tile";
	
}

