package com.mangocity.data.commons.util;
/**
 *
 * @author shilei
 * @date 2016年5月9日 下午1:31:20 
 */
public interface Constants {

	/**
	 * 列表页面正则表时式
	 */
	String LIST_PAGE = "list_page";
	/**
	 * 详情页面正则表达式
	 */
	String DETAIL_PAGE = "detail_page";
	/**
	 * 订单页面正则表达式
	 */
	String ORDER_PAGE = "order_page";
	/**
	 * 订单完成页面正则
	 */
	String ORDER_COMPLETE_PAGE = "order_complete_page";
	
	/**
	 * 游轮列表页面正则表时式
	 */
	String SHIP1_LIST_PAGE = "ship1_list_page";
	/**
	 * 游轮详情页面正则表达式
	 */
	String SHIP1_DETAIL_PAGE = "ship1_detail_page";
	/**
	 * 游轮订单页面正则表达式
	 */
	String SHIP1_ORDER_PAGE = "ship1_order_page";
	/**
	 * 游轮订单完成页面正则
	 */
	String SHIP1_ORDER_COMPLETE_PAGE = "ship1_order_complete_page";
	/**
	 * 页面类型
	 */
	String WEB_PAGE_TYPE = "web_page_type";
	
	/**
	 *  非正常pvurl正则
	 */
	String ILLEGALITY_URL_REGEX = "illegality_url_regex";
	
	/**
	 *  合法的pvurl
	 */
	String LEGAL_URL_REGEX = "legal_url_regex";
	
	String SHIP_CHANNEL = "ship1.mangocity.com";
	
	int ETL_LOAD_TAG = 100;
	
	String CPS = "cps";
	
	String PROJECTCODE = "projectcode";
	String UID = "uid";
	
	/**
	 * 游轮详情页面类型
	 */
	Integer DETAIL_PAGE_TYPE = 32;
	
	/**
	 * 百度渠道
	 */
	String UTM_SOURCE_BAIDU = "bdyoulun";
	String PROJECT_ID_BAIDU = "0020001";
	
	
	String DATE_FORMAT_PATTERN_8 = "yyyyMMdd";
	String DATE_FORMAT_PATTERN_12 = "yyyyMMddHHmm";
	String DATE_FORMAT_PATTERN_14 = "yyyyMMddHHmmss";
	String DATE_FORMAT_PATTERN_17 = "yyyyMMddHHmmssSSS";
	
	String MANGOCITY_TAG = "http://\\w*\\.mangocity\\.com.*";
	
	String CHANNEL_FIELD_NAME = "channel";

	String UTF_8 = "UTF-8";
	
	/** metaq读取消息时写到本地相对路径的字段名  */
	String DATA_FILE_PATH_FIELD = "data_file_path_field";
	/** metaq读取消息时写到本地路径的 默认值  */
	String DATA_FILE_PATH_DEFAULT = "data";
	/** metaq读取消息时写到本地的全路径字段名  */
	String DATA_FILE_PATH = "data_file_path";
	
	
	String DATA_SERVICE_TYPE_WEB = "web";
	
	/** avro文件的后缀名 */
	String AVRO_FILE_TAG = ".avro";
	
}

