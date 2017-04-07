package com.mangocity.etl.util;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;

@SuppressWarnings({ "rawtypes", "static-access", "unchecked" })
public class PropertiesUtil {
	
	private final static Logger LOGGER = Logger.getLogger(PropertiesUtil.class);
	
	/**
	 * 页面标志
	 */
	public final static String PAGE_REGEX_TAG = "_page";
	/**
	 * 渠道标志
	 */
	public final static String CHANNEL_REGEXP_TAG = "_channel_regexp";
	
	/**
	 * 存放页面类型正则的MAP
	 */
	private final static Map<String,String> PAGE_REGEX_MAP = new HashMap<String, String>();
	
	/**
	 * 存放页面类型的MAP
	 */
	private final static Map<String,Integer> WEB_PAGE_TYPE_MAP = new HashMap<String, Integer>();
	
	/**
	 * 存放通道正则表达式MAP
	 */
	private final static Map<String,String> CHANNEL_REGEXP_MAP = new HashMap<String, String>();
	
	/**
	 * 存放通道描述的MAP
	 */
	private final static Map<String,String> CHANNEL_TYPE_MAP = new HashMap<String, String>();
	
	private static Properties prop_config;
	private static JSONArray jsonArray;
	
	static{
		InputStream jsonIn = null;
		InputStream propIn = null;
		try {
			propIn = PropertiesUtil.class.getClassLoader().getResourceAsStream("config.properties");
			prop_config = new Properties();
			prop_config.load(propIn);
			jsonIn = PropertiesUtil.class.getClassLoader().getResourceAsStream("channel.json");
			String jsonStr = IOUtils.toString(jsonIn,Constants.UTF_8);
			List<Map> mapList = jsonArray.parseArray(jsonStr, Map.class);
			for (Map map : mapList) {
				CHANNEL_TYPE_MAP.putAll(map);
			}
		} catch (Exception e) {
			LOGGER.error("load config file error.",e);
		} finally{
			IOUtils.closeQuietly(propIn);
			IOUtils.closeQuietly(jsonIn);
		}
	}
	
	public static String getValue(String name){
		if(StringUtils.isNotBlank(name) && prop_config.containsKey(name)){
			return prop_config.getProperty(name);
		}
		return null;
	}
	
	public static Integer getNumberValue(String name){
		String value = prop_config.getProperty(name);
		if(NumberUtils.isDigits(value)){
			return NumberUtils.toInt(value);
		}
		return 0;
	}
	
	public static Map<String,String> getPageRegexMap(){
		if(PAGE_REGEX_MAP.size()==0){
			for (Map.Entry<Object,Object> entry : prop_config.entrySet()) {
				if(entry.getKey()!=null && entry.getValue()!=null && entry.getKey().toString().endsWith(PAGE_REGEX_TAG)){
					PAGE_REGEX_MAP.put(entry.getKey().toString(), entry.getValue().toString());
				}
			}
		}
		return PAGE_REGEX_MAP;
	}
	
	public static Map<String,Integer> getWebPageTypeMap(){
		if(WEB_PAGE_TYPE_MAP.size()==0){
			String webPageTypeStr = prop_config.getProperty(Constants.WEB_PAGE_TYPE);
			if(StringUtils.isNotBlank(webPageTypeStr)){
				String[] webPageTypes = webPageTypeStr.split(",");
				for (String webPageType : webPageTypes) {
					String[] types = webPageType.split(":");
					if(types.length==2){
						WEB_PAGE_TYPE_MAP.put(types[0], NumberUtils.toInt(types[1], 99));
					}
				}
			}
		}
		return WEB_PAGE_TYPE_MAP;
	}
	
	/**
	 * 渠道正则表达式MAP
	 *
	 * @author shilei
	 * @date 2016年4月22日 下午5:10:18
	 * @return
	 */
	public static Map<String,String> getChannelRegexpMap(){
		if(CHANNEL_REGEXP_MAP.size()==0){
			for (Map.Entry<Object,Object> entry : prop_config.entrySet()) {
				if(entry.getKey()!=null && entry.getValue()!=null && entry.getKey().toString().endsWith(CHANNEL_REGEXP_TAG)){
					String[] channelRegexps = entry.getValue().toString().split(",");
					if(channelRegexps.length<2){
						continue;
					}
					for(int i=1;i<channelRegexps.length;i++){
						CHANNEL_REGEXP_MAP.put(channelRegexps[i], channelRegexps[0]);
					}
				}
			}
		}
		return CHANNEL_REGEXP_MAP;
	}
	
	
	/**
	 * 存放通道描述的MAP
	 * @author shilei
	 * @date 2016年4月25日 上午10:00:49
	 * @return
	 */
	public static String getChannelType(String channelName){
		if(StringUtils.isBlank(channelName)){
			return null;
		}
		return CHANNEL_TYPE_MAP.get(channelName);
	}
	
	
	
	public static void main(String[] args) {
		System.out.println(Constants.ODS_CRUISE_UOL_TABLE_NAME);
	}

}
