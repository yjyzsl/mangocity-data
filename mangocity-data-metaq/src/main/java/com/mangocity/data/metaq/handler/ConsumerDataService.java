package com.mangocity.data.metaq.handler;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableInput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.util.AvroUtil;
import com.mangocity.data.commons.util.GenericUtil;
import com.mangocity.data.commons.util.HttpClientUtil;
import com.mangocity.data.metaq.MetaqConstants;

/**
 * 处理metaq消息具体业务
 * @author shilei
 * @date 2016年5月5日 下午5:18:44 
 */
public abstract class ConsumerDataService<T> {

	@SuppressWarnings("unchecked")
	private Class<T> clazz = (Class<T>)GenericUtil.getGenericType(this.getClass(),0);
	
	
	private final static Logger LOGGER = Logger.getLogger(ConsumerDataService.class);
	
	protected final Object lock = new Object();
	/**
	 * 存放处理去重数据的hash值
	 */
	protected final static Map<Integer,Long> UOL_MAP = new HashMap<Integer,Long>();
	protected final static Map<String,String> PAGE_TITLE_MAP = new HashMap<String,String>();
	
	
	protected abstract Schema getSchema();
	protected abstract T transformUOTL4JSON(JSONObject consumerDataJson);
	
	/**
	 * 检验是否重复
	 * @param hashValue
	 * @return
	 */
	protected boolean checkRepeat(Integer hashValue,Long sourcerowid){
		boolean flag = UOL_MAP.containsKey(hashValue);
		if(!UOL_MAP.containsKey(hashValue)){//加入不重复则添加到缓存中去
			UOL_MAP.put(hashValue,sourcerowid);
		}else{
			LOGGER.debug("conver useroperationlog reduplicate,old sourcerowid:"+UOL_MAP.get(hashValue)+", new sourcerowid:"+sourcerowid);
		}
		return flag;
	}
	
	/**
	 * 获取页面的标题
	 * @param url
	 * @return
	 */
	protected String getPageTitle(String url){
		String pageTile = null;
		if(PAGE_TITLE_MAP.containsKey(url)){
			pageTile = PAGE_TITLE_MAP.get(url);
		}else if(StringUtils.isNotBlank(url)){
			pageTile = HttpClientUtil.getPageTitle(url);
			PAGE_TITLE_MAP.put(url, pageTile);
		}
		return pageTile;
	}
	
	
	
	/**
	 * 进行去重处理操作
	 * @param userOperaTionLogVo
	 * @return
	 */
	protected boolean removeReduplicate(JSONObject uotlJson,Long sourcerowid) {
		//Integer sourcerowid = userOperaTionLogVo.getSourcerowid();
		String mangouid = uotlJson.getString(UOTLField.MANGOUID);
		String pvurl = uotlJson.getString(UOTLField.PVURL);
		String refer = uotlJson.getString(UOTLField.REFER);
		String sessionid = uotlJson.getString(UOTLField.SESSIONID);
		String mbrid = uotlJson.getString(UOTLField.MBRID);
		String operationdt = uotlJson.getString(UOTLField.OPERATIONDT);
		//Long ms = uotlJson.getLong(UOTLField.MS);
		
		StringBuffer hashStr = new StringBuffer();
		if(StringUtils.isNotBlank(mangouid)){
			hashStr.append(mangouid.trim());
		}
		if(StringUtils.isNotBlank(pvurl)){
			hashStr.append(pvurl.trim());
		}
		if(StringUtils.isNotBlank(refer)){
			hashStr.append(refer.trim());
		}
		if(StringUtils.isNotBlank(sessionid)){
			hashStr.append(sessionid.trim());
		}
		if(StringUtils.isNotBlank(mbrid)){
			hashStr.append(mbrid.trim());
		}
		if(StringUtils.isNotBlank(operationdt)){
			hashStr.append(operationdt.trim());
		}
		synchronized (lock) {
			int hashValue = hashStr.toString().hashCode();
			boolean flag = checkRepeat(hashValue,sourcerowid);
			return flag;
		}
	}

	/**
	 * 从refer里取出Projectid
	 * @param userOperaTionLogVo
	 * @return
	 */
	protected String getProjectid(String utm_medium,String utm_source,String refer) {
		String projectID = null;
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		if(paramsMap!=null && paramsMap.containsKey(MetaqConstants.PROJECTCODE)){
			projectID = paramsMap.get(MetaqConstants.PROJECTCODE).toString();
		}
		if(projectID == null && utm_medium!=null && utm_medium.contains("=")){
			projectID = utm_medium.substring(utm_medium.indexOf("=")+1);
		}
		if(projectID==null){
			if(MetaqConstants.UTM_SOURCE_BAIDU.equals(utm_source)){
				projectID = MetaqConstants.PROJECT_ID_BAIDU;
			}
		}
		return projectID;
	}
	
	/**
	 * 从refer里取出uid
	 * @param userOperaTionLogVo
	 * @return
	 */
	protected Integer getUid(String refer) {
		Integer uid = null;
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		if(paramsMap!=null && paramsMap.containsKey(MetaqConstants.UID)){
			String uidStr = paramsMap.get(MetaqConstants.UID);
			if(NumberUtils.isDigits(uidStr)){
				uid = NumberUtils.toInt(uidStr);
			}
		}
		return uid;
	}

	
	
	protected String referToSdtRefer(String refer) {
		String sdtRefer = null;
		if(refer==null){
			return sdtRefer;
		}
		Pattern pattern = Pattern.compile(MetaqConstants.MANGOCITY_TAG);
		Matcher matcher = pattern.matcher(refer);
		boolean flag = matcher.matches();
		if(!flag){
			return sdtRefer;
		}
		if(refer.contains("?")){// 截取?之前的url
			//sdtRefer = refer.substring(0, refer.indexOf("?"));
			sdtRefer = StringUtils.substring(refer, 0, refer.indexOf("?"));
		}else if(refer.contains("#")){// 截取#之前的url
			//sdtRefer = refer.substring(0, refer.indexOf("#"));
			sdtRefer = StringUtils.substring(refer, 0, refer.indexOf("#"));
		}else{
			sdtRefer = refer;
		}
		return sdtRefer;
	}
	
	/**
	 * 将实体bean转化成Avro格式写入到文件中
	 * 如果之前file中存在数据，则写的数据会全部覆盖之前的数据
	 * @author shilei
	 * @date 2016年5月5日 下午5:53:20
	 * @param datas
	 * @param file
	 */
	public boolean writerBeanToAvroFile(List<T> datas,File file){
		Schema schema = this.getSchema();
		return AvroUtil.writerBeanToAvroFile(datas, clazz, schema, file);
	}
	
	
	/**
	 * 将实体bean转化成Avro格式写入到文件中
	 * 如果之前file中存在数据，则写的数据是会在之前的文件上进行追加写入
	 * @author shilei
	 * @date 2016年5月5日 下午5:53:20
	 * @param datas
	 * @param file
	 */
	public boolean appendBeanToAvroFile(List<T> datas,File file){
		Schema schema = this.getSchema();
		return AvroUtil.appendBeanToAvroFile(datas, clazz, schema, file);
	}
	
	/**
	 * 读取file中的数据转化成实体bean
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:57:18
	 * @param file
	 * @return
	 */
	public List<T> readAvroFileToBean(File file){
		return AvroUtil.readAvroFileToBean(file,clazz);
	}
	
	public List<T> readAvroFileToBean(SeekableInput seekableInput){
		return AvroUtil.readAvroFileToBean(seekableInput,clazz);
	}
	
	
}

