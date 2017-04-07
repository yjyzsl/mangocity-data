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

import com.mangocity.etl.service.OdsCruiseUOLService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.util.HttpClientUtil;
import com.mangocity.etl.util.PropertiesUtil;
import com.mangocity.etl.vo.OdsCruiseUOLVo;
import com.mangocity.etl.vo.UserOperaTionLogVo;

/**
 * 游轮处理
 * @author shilei
 *
 */
@Service("shipHandleService")
public class ShipHandleService extends HandleService<OdsCruiseUOLVo> {
	
	private final static Logger LOGGER = Logger.getLogger(ShipHandleService.class);
	
	@Resource(name="odsCruiseUOLService")
	protected OdsCruiseUOLService odsCruiseUOLService;
	
	@Override
	public Integer handle(Integer sourcerowid) {
		UserOperaTionLogVo userOperaTionLogVo = new UserOperaTionLogVo();
		userOperaTionLogVo.setChannel(Constants.SHIP_CHANNEL);
		userOperaTionLogVo.setSourcerowid(sourcerowid);
		userOperaTionLogVo.setOperationtype(1);
		LOGGER.info(userOperaTionLogVo);
		Integer batchSize = PropertiesUtil.getNumberValue(Constants.OTHER_BATCH_SIZE);
		odsCruiseUOLService.setTableName(PropertiesUtil.getValue(Constants.ODS_CRUISE_UOL_TABLE_NAME_FIELD));
		Integer maxSourcerowid = pageHandleUOL(userOperaTionLogVo,batchSize);
		return maxSourcerowid;
	}
	
	
	@Override
	public Integer getMaxSourcerowid(UserOperaTionLogVo userOperaTionLogVo) {
		Integer maxSourcerowid = userOperaTionLogService.findMaxSourceRowIdByChannel(userOperaTionLogVo);
		return maxSourcerowid;
	}

	/**
	 * 将LZ_USEROPERATIONLOG表中的数据处理成ODS_CRUISE_UOL表中的数据
	 * @param userOperaTionLogVo
	 * @return
	 */
	public OdsCruiseUOLVo handleUOL(UserOperaTionLogVo userOperaTionLogVo){
		OdsCruiseUOLVo odsCruiseUOLVo = null;
		if(userOperaTionLogVo == null){
			LOGGER.warn("userOperaTionLogVo is null");
			return odsCruiseUOLVo;
		}
		// 进行去重处理
		boolean flag = removeReduplicate(userOperaTionLogVo);
		// 有重复数据，则不做后续处理
		if(flag){
			return odsCruiseUOLVo;
		}
		String pvurl = userOperaTionLogVo.getPvurl();
		String refer = userOperaTionLogVo.getRefer();
		
		odsCruiseUOLVo = converUOLToCOL(userOperaTionLogVo);
		
		// 从pvurl中取出utm_source，utm_medium，utm_term
		odsCruiseUOLVo.setPvurl(pvurl);
		
		// 将pvurl转化成url
		pvUrlToUrl(odsCruiseUOLVo,pvurl);
		
		// 格式化refer
		String sdtRefer = referToSdtRefer(refer);
		odsCruiseUOLVo.setSdtRefer(sdtRefer);
		
		// 提取UTM的相关字段
		extractUtmField(userOperaTionLogVo,odsCruiseUOLVo);
		
		// refer有cps渠道取出projectcode对应中间表中projectid 
		String utm_medium = odsCruiseUOLVo.getUtm_medium();
		String utm_source = odsCruiseUOLVo.getUtm_source();
		String projectID = getProjectid(utm_medium,utm_source,refer);
		odsCruiseUOLVo.setProjectID(projectID);
		
		String pageTitle = odsCruiseUOLVo.getPageTitle();
		if(StringUtils.isBlank(pageTitle)){
			pageTitle = getPageTitle(pvurl);
			odsCruiseUOLVo.setPageTitle(pageTitle);
		}
		
		// ETL_LOAD_TAG默认是100
		odsCruiseUOLVo.setEtl_load_tag(Constants.ETL_LOAD_TAG); 
		
		// 从refer里获取uid
		Integer uid = getUid(refer);
		odsCruiseUOLVo.setSuid(uid);	                     
		
		return odsCruiseUOLVo;
	}
	
	private void extractUtmField(UserOperaTionLogVo userOperaTionLogVo,OdsCruiseUOLVo odsCruiseUOLVo) {
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
				odsCruiseUOLVo.setSid(NumberUtils.toInt(sid));
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
		odsCruiseUOLVo.setUtm_term(utm_term);
		odsCruiseUOLVo.setUtm_medium(utm_medium);
		odsCruiseUOLVo.setUtm_source(utm_source);
	}
	
	/**
	 * 将pvurl转化成url
	 * @param pvurl
	 * @return
	 */
	protected void pvUrlToUrl(OdsCruiseUOLVo odsCruiseUOLVo,String pvurl) {
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
			if(flag){//找到匹配的表达式，将正则表达式返回
				if(pvurl.contains("?")){// 截取?之前的url
					url = pvurl.substring(0, pvurl.indexOf("?"));
				}else if(pvurl.contains("#")){// 截取#之前的url
					url = pvurl.substring(0, pvurl.indexOf("#"));
				}else{
					url = pvurl;
				}
				Map<String, Integer> webPageTypeMap = PropertiesUtil.getWebPageTypeMap();
				if(webPageTypeMap.containsKey(key)){
					webPageType = webPageTypeMap.get(key);
				}
				break;
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
	public OdsCruiseUOLVo converUOLToCOL(UserOperaTionLogVo userOperaTionLogVo){
		OdsCruiseUOLVo odsCruiseUOLVo = null;
		if(userOperaTionLogVo == null){
			return odsCruiseUOLVo;
		}
		odsCruiseUOLVo = new OdsCruiseUOLVo();
		odsCruiseUOLVo.setSourceRowId(userOperaTionLogVo.getSourcerowid());   
		
		Date userOperationDt = userOperaTionLogVo.getOperationdt();
		// UserOperationDt ->operationdt 进站时间 YYYY-MM-DD HH:MM:SS 
		odsCruiseUOLVo.setUserOperationDt(userOperationDt);        
		// UserOperationTypeId ->operationtype 1-进站数据,2-注册数据,3-点击数据,4-离站数据,5-搜索数据,6-预订数据,7-产品详情页
		odsCruiseUOLVo.setUserOperationTypeId(userOperaTionLogVo.getOperationtype());        
		
		String mbrid = userOperaTionLogVo.getMbrid();
		if(NumberUtils.isDigits(mbrid)){
			odsCruiseUOLVo.setMbrid(NumberUtils.toInt(mbrid));
		}
		odsCruiseUOLVo.setSessionid(userOperaTionLogVo.getSessionid());                  
		odsCruiseUOLVo.setMangouid(userOperaTionLogVo.getMangouid());                   
		odsCruiseUOLVo.setChannel(userOperaTionLogVo.getChannel());                    
		odsCruiseUOLVo.setPvurl(userOperaTionLogVo.getPvurl());                      
		//odsCruiseUOLVo.setUrl(userOperaTionLogVo.getUrl());                        
		odsCruiseUOLVo.setIp(userOperaTionLogVo.getIpaddr());                         
		odsCruiseUOLVo.setCityname(userOperaTionLogVo.getCityname());                   
		odsCruiseUOLVo.setLanguage(userOperaTionLogVo.getLanguage());                   
		odsCruiseUOLVo.setBrowermodel(userOperaTionLogVo.getBrowermodel());                
		odsCruiseUOLVo.setSystemmodel(userOperaTionLogVo.getSystemmodel());                
		odsCruiseUOLVo.setMachinetype(userOperaTionLogVo.getMachinetype());                
		odsCruiseUOLVo.setSearchkeywords(userOperaTionLogVo.getSearchkeywords());             
		odsCruiseUOLVo.setRefer(userOperaTionLogVo.getRefer()); 
		
		String sid = userOperaTionLogVo.getSid();
		if(NumberUtils.isDigits(sid)){
			odsCruiseUOLVo.setSid(NumberUtils.toInt(sid));    
		}
//		odsCruiseUOLVo.setUid(userOperaTionLogVo.getUid());	                     
//		odsCruiseUOLVo.setProjectID(userOperaTionLogVo.getProjectID());                  
		odsCruiseUOLVo.setSearchword(userOperaTionLogVo.getSearchword());                 
		odsCruiseUOLVo.setUtm_term(userOperaTionLogVo.getUtm_term());                   
		odsCruiseUOLVo.setProductid(userOperaTionLogVo.getProductid());  
		// OrderCd -> ordernumber 订单编码 
		odsCruiseUOLVo.setOrderCd(userOperaTionLogVo.getOrdernumber());    
		
		Date sourcerowcreatedt = userOperaTionLogVo.getSourcerowcreatedt();
		odsCruiseUOLVo.setSourceRowCreateDt(sourcerowcreatedt); 
		
		// OperationDayDt,operationDayD,OperationYear,OperationMonth,OperationDay,OperationHour从操作时间中截取
		String sourcerowcreateDtStr = DateFormatUtils.format(sourcerowcreatedt, Constants.DATE_FORMAT_PATTERN_12);
		Long operationDayDt = NumberUtils.toLong(sourcerowcreateDtStr);
		// 分钟分单位
		odsCruiseUOLVo.setOperationDayDt(operationDayDt);  
		
		// 十分钟为单位
		odsCruiseUOLVo.setOperationDayD(operationDayDt/10);  
		
		odsCruiseUOLVo.setOperationYear(operationDayDt/(100*100*100*100));        
		
		odsCruiseUOLVo.setOperationMonth((operationDayDt/(100*100*100))%100); 
		
		odsCruiseUOLVo.setOperationDay((operationDayDt/(100*100))%100);   
		
		odsCruiseUOLVo.setOperationHour((operationDayDt/100)%100);     
		
		// ETL_INSERT_DT,ETL_UPDATE_DT操作数据的插入时间和更新时间
		odsCruiseUOLVo.setEtl_insert_dt(new Date(System.currentTimeMillis()));              
		odsCruiseUOLVo.setEtl_update_dt(new Date(System.currentTimeMillis()));
		
		// add by shilei 20160505
		odsCruiseUOLVo.setStill_time(userOperaTionLogVo.getStill_time());
		odsCruiseUOLVo.setFocus_time(userOperaTionLogVo.getFocus_time());
		odsCruiseUOLVo.setUser_agent(userOperaTionLogVo.getUser_agent());
		odsCruiseUOLVo.setColor_depth(userOperaTionLogVo.getColor_depth());
		odsCruiseUOLVo.setPageTitle(userOperaTionLogVo.getTile());
		odsCruiseUOLVo.setMs(userOperaTionLogVo.getMs());
		odsCruiseUOLVo.setOperation_ms(userOperaTionLogVo.getOperation_ms());
		return odsCruiseUOLVo;
	}

	@Override
	public Integer batchInsert(List<OdsCruiseUOLVo> datas) {
		return this.odsCruiseUOLService.batchInsert(datas);
	}
}
