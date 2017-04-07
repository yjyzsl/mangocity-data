package com.mangocity.etl.util;

/**
 * 定义常量类
 * @author shilei
 *
 */
public interface Constants {
	
	String HANDLE_SERVICE_TYPE = "handle_service_type";
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
	
	String UTM_TERM = "utm_term";
	String UTM_MEDIUM = "utm_medium";
	String UTM_SOURCE = "utm_source";
	
	String SID = "sid";
	
	String SHIP_CHANNEL = "ship1.mangocity.com";
	
	int ETL_LOAD_TAG = 100;
	
	String CPS = "cps";
	
	String PROJECTCODE = "projectcode";
	String UID = "uid";
	
	String BATCH_SIZE = "batch_size";
	String OTHER_BATCH_SIZE = "other_batch_size";
	
	String SCHEDULE_INTERVAL = "schedule_interval";
	
	/**
	 * 游轮详情页面类型
	 */
	Integer DETAIL_PAGE_TYPE = 32;
	
	/**
	 * 百度渠道
	 */
	String UTM_SOURCE_BAIDU = "bdyoulun";
	String PROJECT_ID_BAIDU = "0020001";
	
	String DATE_FORMAT_PATTERN_12 = "yyyyMMddHHmm";
	String DATE_FORMAT_PATTERN_14 = "yyyyMMddHHmmss";
	
	String MANGOCITY_TAG = "http://\\w*\\.mangocity\\.com.*";
	
	String CHANNEL_FIELD_NAME = "channel";
	/**
	 * 分页查询时当前ID字段名
	 */
	String CURRENTROWID_FIELD_NAME = "currentRowId";
	/**
	 * 分页查询时下页ID字段名
	 */
	String NEXTROWID_FIELD_NAME = "nextRowId";
	
	String SOURCE_ROW_ID_FIELD_NAME = "sourceRowId";
	//84893664
	/**
	 * 游轮渠道
	 */
	String ODS_CRUISE_UOL_TABLE_NAME = "ods.ODS_Cruise_UOL";
	String ODS_CRUISE_UOL_TABLE_NAME_FIELD = "ods_cruise_uol_table_name";
//	String ODS_CRUISE_UOL_TABLE_NAME = "ods.ODS_Cruise_UOL";
//	String ODS_CRUISE_UOL_TABLE_NAME = "lz.ODS_CRUISE_UOL_TEST";
	
	/**
	 * 全部渠道
	 */
	String ODS_AS_UOL_TABLE_NAME = "ods.ODS_AS_UOL";
	String ODS_AS_UOL_TABLE_NAME_FIELD = "ods_as_uol_table_name";

	String UTF_8 = "UTF-8";
}
