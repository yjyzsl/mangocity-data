package com.mangocity.etl.handle;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.mangocity.etl.service.OdsAsUOLService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.util.HttpClientUtil;
import com.mangocity.etl.util.PropertiesUtil;
import com.mangocity.etl.vo.OdsAsUOLVo;
import com.mangocity.etl.vo.UserOperaTionLogVo;

/**
 * 所有渠道
 * @author shilei
 *
 */
@Service("channelAllHandleService")
public class ChannelAllHandleService extends HandleService<OdsAsUOLVo> {
	
	private final static Logger LOGGER = Logger.getLogger(ChannelAllHandleService.class);

	@Resource(name="odsAsUOLService")
	protected OdsAsUOLService odsAsUOLService;
	
	@Override
	public Integer handle(Integer sourcerowid) {
		UserOperaTionLogVo userOperaTionLogVo = new UserOperaTionLogVo();
		userOperaTionLogVo.setSourcerowid(sourcerowid);
		userOperaTionLogVo.setOperationtype(1);
		LOGGER.info(userOperaTionLogVo);
		Integer batchSize = PropertiesUtil.getNumberValue(Constants.BATCH_SIZE);
		// 设置表面
		odsAsUOLService.setTableName(PropertiesUtil.getValue(Constants.ODS_AS_UOL_TABLE_NAME_FIELD));
		Integer maxSourcerowid = pageHandleUOL(userOperaTionLogVo,batchSize);
		return maxSourcerowid;
	}
	
	@Override
	public Integer getMaxSourcerowid(UserOperaTionLogVo userOperaTionLogVo) {
		Integer maxSourcerowid = userOperaTionLogService.findMaxSourceRowId();
		return maxSourcerowid;
	}
	
	@Override
	public Integer batchInsert(List<OdsAsUOLVo> datas) {
		return this.odsAsUOLService.batchInsert(datas);
	}

	/**
	 * 将LZ_USEROPERATIONLOG表中的数据处理成ODS_CRUISE_UOL表中的数据
	 * @param userOperaTionLogVo
	 * @return
	 */
	public OdsAsUOLVo handleUOL(UserOperaTionLogVo userOperaTionLogVo){
		OdsAsUOLVo odsAsUOLVo = null;
		if(userOperaTionLogVo == null){
			LOGGER.warn("userOperaTionLogVo is null");
			return odsAsUOLVo;
		}
		// 进行去重处理
		boolean flag = removeReduplicate(userOperaTionLogVo);
		// 有重复数据，则不做后续处理
		if(flag){
			return odsAsUOLVo;
		}
		String pvurl = userOperaTionLogVo.getPvurl();
		String refer = userOperaTionLogVo.getRefer();
		
		odsAsUOLVo = converUOLToCOL(userOperaTionLogVo);
		
		// 从pvurl中取出utm_source，utm_medium，utm_term
		odsAsUOLVo.setPvurl(pvurl);
		
		// 将pvurl转化成url
		pvUrlToUrl(odsAsUOLVo,pvurl);
		
		// 格式化refer
		String sdtRefer = referToSdtRefer(refer);
		odsAsUOLVo.setSdtRefer(sdtRefer);
		
		// 提取UTM的相关字段
		extractUtmField(userOperaTionLogVo,odsAsUOLVo);
		
		//设置渠道类型和站定来源类型
		sourcesAndSiteType4Channel(odsAsUOLVo);
		
		// refer有cps渠道取出projectcode对应中间表中projectid 
		String utm_medium = odsAsUOLVo.getUtm_medium();
		String utm_source = odsAsUOLVo.getUtm_source();
		String projectID = getProjectid(utm_medium,utm_source,refer);
		odsAsUOLVo.setProjectID(projectID);
		
		String pageTitle = odsAsUOLVo.getPageTitle();
		if(StringUtils.isBlank(pageTitle)){
			pageTitle = getPageTitle(pvurl);
			odsAsUOLVo.setPageTitle(pageTitle);
		}
		
		// ETL_LOAD_TAG默认是100
		odsAsUOLVo.setEtl_load_tag(Constants.ETL_LOAD_TAG); 
		
		// 从refer里获取uid
		Integer uid = getUid(refer);
		odsAsUOLVo.setSuid(uid);	                     
		
		return odsAsUOLVo;
	}
	
	/**
	 * 设置渠道类型和站定来源类型(Web,App,H5)
	 * @author shilei
	 * @date 2016年4月25日 上午10:08:02
	 * @param odsAsUOLVo
	 * @return
	 */
	public static void sourcesAndSiteType4Channel(OdsAsUOLVo odsAsUOLVo) {
		String channel = odsAsUOLVo.getChannel();
		if(StringUtils.isBlank(channel)){
			return ;
		}
		String sourcesType = PropertiesUtil.getChannelType(channel);
		//设置通道类型
		odsAsUOLVo.setSourcesType(sourcesType);
		
		String siteType = null;
		Map<String,String> channelRegexpMap = PropertiesUtil.getChannelRegexpMap();
		Pattern pattern = null;
		Matcher matcher = null;
		boolean flag = false;
		for (Map.Entry<String, String> entry : channelRegexpMap.entrySet()) {
			String pageRegex = entry.getKey();
			pattern = Pattern.compile(pageRegex);
			matcher = pattern.matcher(channel);
			flag = matcher.matches();
			if(flag){//找到匹配的表达式，将正则表达式返回
				siteType = entry.getValue();
				break;
			}
		}
		odsAsUOLVo.setSiteType(siteType);
	}
	

	private void extractUtmField(UserOperaTionLogVo userOperaTionLogVo,OdsAsUOLVo odsAsUOLVo) {
		String utm_term = userOperaTionLogVo.getUtm_term();
		String utm_medium = userOperaTionLogVo.getUtm_medium();
		String utm_source = userOperaTionLogVo.getUtm_source();
		//http://ship1.mangocity.com/cruise-line_2_0_0_0_0_0.html?utm_source=bdyoulun&utm_medium=cpc&utm_term=baidu
		String pvurl = userOperaTionLogVo.getPvurl();
		String refer = userOperaTionLogVo.getRefer();
		// 取pvurl上的参数
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(pvurl);
		
		if(paramsMap==null || paramsMap.size()<3){
			// 取refer上的参数
			paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		}
		if(paramsMap!=null){
			if(paramsMap.containsKey(Constants.UTM_TERM)){
				utm_term = paramsMap.get(Constants.UTM_TERM);
			}
			if(paramsMap.containsKey(Constants.UTM_MEDIUM)){
				utm_medium = paramsMap.get(Constants.UTM_MEDIUM);
			}
			if(paramsMap.containsKey(Constants.UTM_SOURCE)){
				utm_source = paramsMap.get(Constants.UTM_SOURCE);
			}
			if(paramsMap.containsKey(Constants.SID)){
				String sid = paramsMap.get(Constants.SID);
				odsAsUOLVo.setSid(NumberUtils.toInt(sid));
			}
		}
		// 对关键字进行解码操作
		if(StringUtils.isNoneBlank(utm_term)){
			try {
				URLDecoder.decode(utm_term, Constants.UTF_8);
			} catch (Exception e) {
				LOGGER.warn("decode fial sourcerowid "+userOperaTionLogVo.getSourcerowid()+" utm_term:"+utm_term, e);
			}
		}
		odsAsUOLVo.setUtm_term(utm_term);
		odsAsUOLVo.setUtm_medium(utm_medium);
		odsAsUOLVo.setUtm_source(utm_source);
	}
	
	
	/**
	 * 将pvurl转化成url
	 * @param pvurl
	 * @return
	 */
	protected void pvUrlToUrl(OdsAsUOLVo odsCruiseUOLVo,String pvurl) {
		if(StringUtils.isBlank(pvurl)){
			return ;
		}
		Pattern pattern = null;
		Matcher matcher = null;
		boolean flag = false;
		String url = pvurl;
		Integer webPageType = 99;
		Map<String,String> pageRegexMap =  PropertiesUtil.getPageRegexMap();
		for (Map.Entry<String, String> entry : pageRegexMap.entrySet()) {
			String key = entry.getKey();
			String pageRegex = entry.getValue();
			pattern = Pattern.compile(pageRegex);
			matcher = pattern.matcher(pvurl);
			flag = matcher.matches();
			if(flag){//找到匹配的表达式，将正则表达式返回（目前只统计游轮的页面类型）
				Map<String, Integer> webPageTypeMap = PropertiesUtil.getWebPageTypeMap();
				if(webPageTypeMap.containsKey(key)){
					webPageType = webPageTypeMap.get(key);
				}
				break;
			}
			if(pvurl.contains("?")){// 截取?之前的url
				url = pvurl.substring(0, pvurl.indexOf("?"));
			}else if(pvurl.contains("#")){// 截取#之前的url
				url = pvurl.substring(0, pvurl.indexOf("#"));
			}else{
				//验证pvurl是不是非法pvurl
				pattern = Pattern.compile(PropertiesUtil.getValue(Constants.LEGAL_URL_REGEX));
				matcher = pattern.matcher(pvurl);
				flag = matcher.matches();
				if(!flag){//不是合法的pvurl
					pattern = Pattern.compile(PropertiesUtil.getValue(Constants.ILLEGALITY_URL_REGEX));
					matcher = pattern.matcher(pvurl);
					if(matcher.matches()){//不是合法的pvurl
						url = null;
					}
				}
			}
		}
		if(webPageType==Constants.DETAIL_PAGE_TYPE){//详情页面
			// 提取产品ID
			String productid = url.substring(url.indexOf("-")+1, url.indexOf(".html"));
			odsCruiseUOLVo.setProjectID(productid);		
		}
		odsCruiseUOLVo.setUrl(url);
		odsCruiseUOLVo.setWebpagetypeid(webPageType);
	}
	
	
	/**
	 * 用户操作日志对象转化成中间表对象
	 * @param userOperaTionLogVo
	 * @return
	 */
	public OdsAsUOLVo converUOLToCOL(UserOperaTionLogVo userOperaTionLogVo){
		OdsAsUOLVo odsAsUOLVo = null;
		if(userOperaTionLogVo == null){
			return odsAsUOLVo;
		}
		odsAsUOLVo = new OdsAsUOLVo();
		odsAsUOLVo.setSourceRowId(userOperaTionLogVo.getSourcerowid());   
		
		Date userOperationDt = userOperaTionLogVo.getOperationdt();
		// UserOperationDt ->operationdt 进站时间 YYYY-MM-DD HH:MM:SS 
		odsAsUOLVo.setUserOperationDt(userOperationDt);        
		// UserOperationTypeId ->operationtype 1-进站数据,2-注册数据,3-点击数据,4-离站数据,5-搜索数据,6-预订数据,7-产品详情页
		odsAsUOLVo.setUserOperationTypeId(userOperaTionLogVo.getOperationtype());        
		
		String mbrid = userOperaTionLogVo.getMbrid();
		if(NumberUtils.isDigits(mbrid)){
			odsAsUOLVo.setMbrid(NumberUtils.toInt(mbrid));
		}
		odsAsUOLVo.setSessionid(userOperaTionLogVo.getSessionid());                  
		odsAsUOLVo.setMangouid(userOperaTionLogVo.getMangouid());                   
		odsAsUOLVo.setChannel(userOperaTionLogVo.getChannel());                    
		odsAsUOLVo.setPvurl(userOperaTionLogVo.getPvurl());                      
		//odsAsUOLVo.setUrl(userOperaTionLogVo.getUrl());                        
		odsAsUOLVo.setIp(userOperaTionLogVo.getIpaddr());                         
		odsAsUOLVo.setCityname(userOperaTionLogVo.getCityname());                   
		odsAsUOLVo.setLanguage(userOperaTionLogVo.getLanguage());                   
		odsAsUOLVo.setBrowermodel(userOperaTionLogVo.getBrowermodel());                
		odsAsUOLVo.setSystemmodel(userOperaTionLogVo.getSystemmodel());                
		odsAsUOLVo.setMachinetype(userOperaTionLogVo.getMachinetype());                
		odsAsUOLVo.setSearchkeywords(userOperaTionLogVo.getSearchkeywords());             
		odsAsUOLVo.setRefer(userOperaTionLogVo.getRefer()); 
		
		String sid = userOperaTionLogVo.getSid();
		if(NumberUtils.isDigits(sid)){
			odsAsUOLVo.setSid(NumberUtils.toInt(sid));    
		}
//		odsAsUOLVo.setUid(userOperaTionLogVo.getUid());	                     
//		odsAsUOLVo.setProjectID(userOperaTionLogVo.getProjectID());                  
		odsAsUOLVo.setSearchword(userOperaTionLogVo.getSearchword());                 
		odsAsUOLVo.setUtm_term(userOperaTionLogVo.getUtm_term());                   
		odsAsUOLVo.setProductid(userOperaTionLogVo.getProductid());  
		// OrderCd -> ordernumber 订单编码 
		odsAsUOLVo.setOrderCd(userOperaTionLogVo.getOrdernumber());    
		
		Date sourcerowcreatedt = userOperaTionLogVo.getSourcerowcreatedt();
		odsAsUOLVo.setSourceRowCreateDt(sourcerowcreatedt); 
		
		// OperationDayDt,operationDayD,OperationYear,OperationMonth,OperationDay,OperationHour从操作时间中截取
		String sourcerowcreateDtStr = DateFormatUtils.format(sourcerowcreatedt, Constants.DATE_FORMAT_PATTERN_12);
		Long operationDayDt = NumberUtils.toLong(sourcerowcreateDtStr);
		// 分钟分单位
		odsAsUOLVo.setOperationDayDt(operationDayDt);  
		
		// 十分钟为单位
		odsAsUOLVo.setOperationDayD(operationDayDt/10);  
		
		odsAsUOLVo.setOperationYear(operationDayDt/(100*100*100*100));        
		
		odsAsUOLVo.setOperationMonth((operationDayDt/(100*100*100))%100); 
		
		odsAsUOLVo.setOperationDay((operationDayDt/(100*100))%100);   
		
		odsAsUOLVo.setOperationHour((operationDayDt/100)%100);     
		
		// ETL_INSERT_DT,ETL_UPDATE_DT操作数据的插入时间和更新时间
		odsAsUOLVo.setEtl_insert_dt(new Date(System.currentTimeMillis()));              
		odsAsUOLVo.setEtl_update_dt(new Date(System.currentTimeMillis()));
		
		// add by shilei 20160505
		odsAsUOLVo.setStill_time(userOperaTionLogVo.getStill_time());
		odsAsUOLVo.setFocus_time(userOperaTionLogVo.getFocus_time());
		odsAsUOLVo.setUser_agent(userOperaTionLogVo.getUser_agent());
		odsAsUOLVo.setColor_depth(userOperaTionLogVo.getColor_depth());
		odsAsUOLVo.setPageTitle(userOperaTionLogVo.getTile());
		odsAsUOLVo.setMs(userOperaTionLogVo.getMs());
		odsAsUOLVo.setOperation_ms(userOperaTionLogVo.getOperation_ms());
		return odsAsUOLVo;
	}


}
