package com.mangocity.data.metaq.handler;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.util.XmlUtil;
import com.mangocity.data.metaq.MetaqConstants;
import com.taobao.metamorphosis.utils.codec.impl.JavaDeserializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;

/**
 * 解析消息转化成实体bean
 * @author shilei
 * @date 2016年5月6日 下午2:16:31 
 */
public class ParseMsg2Bean {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ParseMsg2Bean.class);
	// metaq中的消息转化成实体对象对应的映射关系
	private final static List<Map<String,String>> RELATION = new ArrayList<Map<String,String>>(); 
	
	private final static JavaDeserializer DESERIALIZER = new JavaDeserializer();
	
	static{
		try {
			InputStream relationIn = ParseMsg2Bean.class.getClassLoader().getResourceAsStream("conf/relation.xml");
			List<Map<String,String>> relationTmp = XmlUtil.parseXml(relationIn);
			if(CollectionUtils.isNotEmpty(relationTmp)){
				RELATION.addAll(relationTmp);
				relationTmp.clear();
			}
			relationTmp = null;
		} catch (DocumentException e) {
			LOGGER.debug("", e);
		}
	}
	
	/**
	 * 消息转化成对象
	 *
	 * @author shilei
	 * @date 2016年5月9日 上午10:59:36
	 * @param msg
	 * @return
	 */
	public static <T> T msg2Bean(String msg,Class<T> clazz){
		JSONObject jsonObject = msg2Json(msg);
		T bean = JSONObject.toJavaObject(jsonObject,clazz);
		return bean;
	}
	
	
	/**
	 * 消息转化成Json
	 *
	 * @author shilei
	 * @date 2016年5月9日 上午10:59:36
	 * @param msg
	 * @return
	 */
	public static JSONObject msg2Json(String msg){
		JSONObject jsonObject = null;
		try {
			jsonObject = JSONObject.parseObject(msg);
		} catch (JSONException e) {
			try {
				msg = (String)DESERIALIZER.decodeObject(msg.getBytes());
				jsonObject = JSONObject.parseObject(msg);
			} catch (Exception e1) {
				LOGGER.error("msg2Json error msg :{} ",msg, e1);
			}
		}
		if(jsonObject == null){
			LOGGER.debug("msg2Json error , jsonObject is null , msg {}.", msg);
			return jsonObject;
		}
		JSONObject valueJson = new JSONObject();
		moveValue(valueJson,jsonObject);
		
		for(Map<String,String> relationMap:RELATION){//遍历所有列
			String jsonKey = relationMap.get(MetaqConstants.JSONKEY);
			String columnKey = relationMap.get(MetaqConstants.COLUMNKEY);
			if(!"businesschannel".equals(jsonKey) && (columnKey.equals(jsonKey) || StringUtils.isBlank(jsonKey))){
				continue;
			}
			Object columnValue = operationValue(relationMap,valueJson);
			valueJson.put(columnKey, columnValue);
		}
		jsonObject.clear();
		jsonObject = null;
		return valueJson;
	}
	

	private static void moveValue(JSONObject valueJson,JSONObject jsonObject){
		for(Map.Entry<String, Object> entry:jsonObject.entrySet()){
			String key = entry.getKey();
			Object value = entry.getValue();
			moveValue(valueJson,key,value);
		}
	}
	
	private static void moveValue(JSONObject valueJson,String key,Object value){
		if(value == null || StringUtils.isBlank(value.toString()) || "null".equals(value)){
			return;
		}
		JSONObject tmpJson = null;
		if(value instanceof JSONObject){
			tmpJson = (JSONObject)value;
			moveValue(valueJson,tmpJson);
		}else if(value instanceof JSONArray){
			JSONArray jsonArray = (JSONArray)value;
			for (Object object : jsonArray) {
				moveValue(valueJson,key,object);
			}
		}else if(value instanceof Map){
			tmpJson = new JSONObject((Map)value);
			moveValue(valueJson,tmpJson);
		}else if(!MetaqConstants.DATA_FIELD.equals(key)){
			key = key.toLowerCase().trim();
			valueJson.put(key, value);
		}else{
			try {
				tmpJson = JSONObject.parseObject(value.toString());
				moveValue(valueJson,tmpJson);
			} catch (Exception e) {
				LOGGER.debug("key {} , value {}", key,value);
			}
		}
	}
	

	/**
	 * 对字段进行格式化转换
	 *
	 * @author shilei
	 * @date 2016年5月6日 下午5:29:01
	 * @param operation
	 * @param key
	 * @param jsonObject
	 * @return
	 */
	public static Object operationValue(Map<String,String> relationMap, JSONObject jsonObject) {
		String jsonKey = relationMap.get(MetaqConstants.JSONKEY);
		String operation =  relationMap.get(MetaqConstants.OPERATION);
		Object value = getJsonValue(jsonKey, jsonObject);
		if (value != null && StringUtils.isNoneBlank(value.toString()) && !"null".equals(value.toString())) {
			value = value.toString().replace("'", "").trim();
			String[] ops = StringUtils.split(operation, ",");
			for (String op : ops) {// 这中类型不能同时存在，每个字段有且只有一种
				switch (op) {
				case MetaqConstants.DATE_TO_LONG:
					String patternStr = relationMap.get(MetaqConstants.PATTERN);
					String[] patterns = StringUtils.split(patternStr,",");
					if(value.toString().contains("HH")){
						value = StringUtils.replace(value.toString(), "HH", "00");
					}else if(value.toString().contains("null")){
						value = DateFormatUtils.format(System.currentTimeMillis(), MetaqConstants.DATE_FORMAT_PATTERN_14);
						break;
					}
					boolean flag = true;
					for (String pattern : patterns) {
						try {
							Date date = DateUtils.parseDate(value.toString(), pattern);
							value = DateFormatUtils.format(date, MetaqConstants.DATE_FORMAT_PATTERN_14);
						} catch (ParseException e) {
							flag = false;
						}
						if(flag){
							break;
						}
					}
					break;
				case MetaqConstants.TO_DECODE:
					try {
						value = URLDecoder.decode(value.toString(), MetaqConstants.UTF_8);
					} catch (UnsupportedEncodingException e) {
					}
					break;
				default:
					break;
				}
			}
		}
		return value;
	}
	
	/**
	 * jsonkey为多个值的情况，遍历从json对象中寻找值
	 *
	 * @author shilei
	 * @date 2016年5月6日 下午5:27:59
	 * @param jsonKey
	 * @param jsonObject
	 * @return
	 */
	public static Object getJsonValue(String jsonKey, JSONObject jsonObject) {
		String[] keys = StringUtils.split(jsonKey, ",");
		Object value = null;
		for (String key : keys) {
			value = jsonObject.get(key);
			if (value!=null && StringUtils.isNoneBlank(value.toString())) {
				break;
			}
		}
		return value;
	}
	
	
	public static void main(String[] args) {
		String pattern = "yyyy/MM/dd HH:mm:ss";
		String value = "2016/4/1 9:46:17";
		//String.format(format, args);
				
		if(value.toString().contains("HH")){
			value = StringUtils.replace(value.toString(), "HH", "00");
		}else if(value.toString().contains("null")){
			value = DateFormatUtils.format(System.currentTimeMillis(), pattern);
		}
		try {
			Date date = DateUtils.parseDate(value.toString(), pattern);
			value = DateFormatUtils.format(date, MetaqConstants.DATE_FORMAT_PATTERN_14);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println(value);
	}
}

