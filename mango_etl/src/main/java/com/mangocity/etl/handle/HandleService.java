package com.mangocity.etl.handle;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.mangocity.etl.service.UserOperaTionLogService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.util.HttpClientUtil;
import com.mangocity.etl.vo.UserOperaTionLogVo;

public abstract class HandleService<T> {
	
	private final static Logger LOGGER = Logger.getLogger(HandleService.class);
	/**
	 * 存放处理去重数据的hash值
	 */
	protected final static Map<Integer,Integer> UOL_MAP = new HashMap<Integer,Integer>();
	protected final static Map<String,String> PAGE_TITLE_MAP = new HashMap<String,String>();
	
	@Resource(name="userOperaTionLogService")
	protected UserOperaTionLogService userOperaTionLogService;
	
	/**
	 * 检验是否重复
	 * @param hashValue
	 * @return
	 */
	protected boolean checkRepeat(Integer hashValue,Integer sourceRowId){
		boolean flag = UOL_MAP.containsKey(hashValue);
		if(!UOL_MAP.containsKey(hashValue)){//加入不重复则添加到缓存中去
			UOL_MAP.put(hashValue,sourceRowId);
		}else{
			LOGGER.info("conver useroperationlog reduplicate,old sourceRowId:"+UOL_MAP.get(hashValue)+", new sourceRowId:"+sourceRowId);
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
	
	public abstract Integer handle(Integer sourcerowid);
	
	public abstract Integer batchInsert(List<T> datas);
	
	public abstract T handleUOL(UserOperaTionLogVo userOperaTionLogVo);

	public abstract Integer getMaxSourcerowid(UserOperaTionLogVo userOperaTionLogVo);
	/**
	 * 用sourceRowId来分页
	 * @return
	 */
	protected Integer pageHandleUOL(UserOperaTionLogVo userOperaTionLogVo,Integer batchSize){
		//计算最大的ID
		Integer maxSourcerowid = getMaxSourcerowid(userOperaTionLogVo);
		Integer currentRowId = userOperaTionLogVo.getSourcerowid();
		if(currentRowId<0){
			currentRowId = userOperaTionLogService.findMinSourceRowIdByChannel(userOperaTionLogVo);
		}
		LOGGER.info("current sourcerowid:"+currentRowId+", max sourcerowid:"+maxSourcerowid);
		Integer offset = currentRowId;
		Integer nextRowId = 0;
		Integer maxRowId = -1;
		Integer rowId = -1;
		//用ID来分页
		while((nextRowId=offset+batchSize) <= maxSourcerowid){
			rowId = batchHandleUOL(userOperaTionLogVo,offset,nextRowId);
			if(rowId > maxRowId){
				maxRowId = rowId;
			}
			offset = nextRowId;
		}
		if(offset < maxSourcerowid){
			rowId = batchHandleUOL(userOperaTionLogVo,offset,maxSourcerowid);
			if(rowId > maxRowId){
				maxRowId = rowId;
			}
		}
		return maxRowId;
	}
	
	
	/**
	 * 用户操作日志数据转化成中间表数据
	 * @param userOperaTionLogVo
	 * @param currentRowId
	 * @param nextRowId
	 * @return
	 */
	private Integer batchHandleUOL(UserOperaTionLogVo userOperaTionLogVo,Integer currentRowId,Integer nextRowId){
		Map<String,Object> params = new HashMap<String, Object>();
		params.put(Constants.CHANNEL_FIELD_NAME, userOperaTionLogVo.getChannel()); 
		params.put(Constants.CURRENTROWID_FIELD_NAME, currentRowId); 
		params.put(Constants.NEXTROWID_FIELD_NAME, nextRowId); 
		params.put("operationtype", userOperaTionLogVo.getOperationtype()); 
		List<UserOperaTionLogVo> userOperaTionLogVoList = userOperaTionLogService.findPageBySourcerowid(params);
		
		Integer maxSourcerowid = currentRowId;
		if(CollectionUtils.isEmpty(userOperaTionLogVoList)){
			LOGGER.info("find useroperationlog is empty,params:"+params);
			return maxSourcerowid;
		}
		LOGGER.info("handle useroperationlog list size:"+userOperaTionLogVoList.size());
		List<T> voList = new ArrayList<T>();
		T vo = null;
		for (UserOperaTionLogVo operaTionLogVo : userOperaTionLogVoList) {
			try {
				// 将LZ_USEROPERATIONLOG表中的数据处理成ODS_CRUISE_UOL表中的数据
				vo = handleUOL(operaTionLogVo);
				if(operaTionLogVo == null){
					continue;
				}
				if(vo==null){
					continue;
				}
				Integer sourcerowid = operaTionLogVo.getSourcerowid();
				if(sourcerowid > maxSourcerowid){
					maxSourcerowid = sourcerowid;
				}
				//LOGGER.info("handle ods_cruise_uol sourceRowId:"+sourcerowid);
				voList.add(vo);
			} catch (Exception e) {
				LOGGER.error("handle fail sourcerowid:"+operaTionLogVo.getSourcerowid(), e);
			}
		}
		LOGGER.info("handle ods_cruise_uol batchInsert size:"+voList.size());
		if(voList.size()>0){
			// 批量插入到数据库
			this.batchInsert(voList);
			voList.clear();
		}
		return maxSourcerowid;
	}
	
	
	/**
	 * 进行去重处理操作
	 * @param userOperaTionLogVo
	 * @return
	 */
	protected boolean removeReduplicate(UserOperaTionLogVo userOperaTionLogVo) {
		Integer sourcerowid = userOperaTionLogVo.getSourcerowid();
		String mangouid = userOperaTionLogVo.getMangouid();
		String pvurl = userOperaTionLogVo.getPvurl();
		String refer = userOperaTionLogVo.getRefer();
		String sessionid = userOperaTionLogVo.getSessionid();
		String mbrid = userOperaTionLogVo.getMbrid();
		Date operationdt = userOperaTionLogVo.getOperationdt();
		String operationdtStr = DateFormatUtils.format(operationdt, Constants.DATE_FORMAT_PATTERN_14);
		
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
		if(StringUtils.isNotBlank(operationdtStr)){
			hashStr.append(operationdtStr.trim());
		}
		int hashValue = hashStr.toString().hashCode();
		boolean flag = checkRepeat(hashValue,sourcerowid);
		return flag;
	}

	/**
	 * 从refer里取出Projectid
	 * @param userOperaTionLogVo
	 * @return
	 */
	protected String getProjectid(String utm_medium,String utm_source,String refer) {
		String projectID = null;
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		if(paramsMap!=null && paramsMap.containsKey(Constants.PROJECTCODE)){
			projectID = paramsMap.get(Constants.PROJECTCODE).toString();
		}
		if(projectID == null && utm_medium!=null && utm_medium.contains("=")){
			projectID = utm_medium.substring(utm_medium.indexOf("=")+1);
		}
		if(projectID==null){
			if(Constants.UTM_SOURCE_BAIDU.equals(utm_source)){
				projectID = Constants.PROJECT_ID_BAIDU;
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
		if(paramsMap!=null && paramsMap.containsKey(Constants.UID)){
			String uidStr = paramsMap.get(Constants.UID);
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
		Pattern pattern = Pattern.compile(Constants.MANGOCITY_TAG);
		Matcher matcher = pattern.matcher(refer);
		boolean flag = matcher.matches();
		if(!flag){
			return sdtRefer;
		}
		if(refer.contains("?")){// 截取?之前的url
			sdtRefer = refer.substring(0, refer.indexOf("?"));
		}else if(refer.contains("#")){// 截取#之前的url
			sdtRefer = refer.substring(0, refer.indexOf("#"));
		}else{
			sdtRefer = refer;
		}
		return sdtRefer;
	}

	
	
}
