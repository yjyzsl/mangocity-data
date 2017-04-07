package com.mangocity.data.metaq.handler.impl;

import java.net.URLDecoder;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.bean.UserOperaTionLogBean;
import com.mangocity.data.commons.util.HttpClientUtil;
import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.metaq.MetaqConstants;
import com.mangocity.data.metaq.handler.ConsumerDataService;
import com.mangocity.data.metaq.handler.UOTLField;

/**
 *
 * @author shilei
 * @date 2016年5月5日 下午5:27:44 
 */
@Repository("uOALConsumerDataServiceImpl")
public class UOALConsumerDataServiceImpl extends ConsumerDataService<UserOperaTionLogBean>{

	private final static Logger LOGGER = LoggerFactory.getLogger(UOALConsumerDataServiceImpl.class);
	
	@Override
	protected Schema getSchema() {
		return UserOperaTionLogBean.getClassSchema();
	}
	
	/**
	 * 将LZ_USEROPERATIONLOG表中的数据处理成ODS_CRUISE_UOL表中的数据
	 * @param userOperaTionLogVo
	 * @return
	 */
	@Override
	public UserOperaTionLogBean transformUOTL4JSON(final JSONObject uotlJson){
		UserOperaTionLogBean userOperaTionLogBean = null;
		if(uotlJson == null || uotlJson.isEmpty()){
			LOGGER.warn("userOperaTionLog json is null");
			return userOperaTionLogBean;
		}
		//生成sourcerowid,作为唯一标识
		Long sourcerowid = createSourcerowid();
		// 进行去重处理
		boolean flag = removeReduplicate(uotlJson,sourcerowid);
		// 有重复数据，则不做后续处理
		if(flag){
			return userOperaTionLogBean;
		}
		String pvurl = uotlJson.getString(UOTLField.PVURL);
		String refer = uotlJson.getString(UOTLField.REFER);
		
		try {
			userOperaTionLogBean =  JSONObject.toJavaObject(uotlJson, UserOperaTionLogBean.class);
		} catch (Exception e) {
			LOGGER.error("toJavaObject error {}", uotlJson);
			LOGGER.error("{}", e);
			return userOperaTionLogBean;
		}
		// 将pvurl转化成url
		pvUrlToUrl(userOperaTionLogBean,pvurl);
		
		// 格式化refer
		String sdtRefer = referToSdtRefer(refer);
		userOperaTionLogBean.setSdtrefer(sdtRefer);
		
		// 提取UTM的相关字段
		extractUtmField(userOperaTionLogBean);
		
		//设置渠道类型和站定来源类型
		sourcesAndSiteType4Channel(userOperaTionLogBean);
		
		// refer有cps渠道取出projectcode对应中间表中projectid 
		String utm_medium = userOperaTionLogBean.getUtmMedium()==null?null:(String)userOperaTionLogBean.getUtmMedium();
		String utm_source = userOperaTionLogBean.getUtmSource()==null?null:(String)userOperaTionLogBean.getUtmSource();
		CharSequence projectID = userOperaTionLogBean.getProjectid();
		if(StringUtils.isBlank(projectID)){
			projectID = getProjectid(utm_medium,utm_source,refer);
			userOperaTionLogBean.setProjectid(projectID);
		}
		
		String pageTitle = userOperaTionLogBean.getTitle()==null?null:(String)userOperaTionLogBean.getUtmSource();
		// 是否需要通过http请求获取标题
		boolean httpPageTile = PropertiesUtil.getBooleanValue(MetaqConstants.HTTP_PAGE_TILE, MetaqConstants.HTTP_PAGE_TILE_VALUE);
		if(httpPageTile && StringUtils.isBlank(pageTitle)){
			pageTitle = getPageTitle(pvurl);
			userOperaTionLogBean.setTitle(pageTitle);
		}
		
		// 从refer里获取uid
		Integer uid = getUid(refer);
		userOperaTionLogBean.setSuid(uid);                
		
		//设置转化操作时间
		transOperaTionTime(userOperaTionLogBean);
		
		userOperaTionLogBean.setSourcerowid(sourcerowid);
		
		return userOperaTionLogBean;
	}
	
	/**
	 * 
	 * 生成sourcerowid,作为唯一标识
	 * @author shilei
	 * @date 2016年5月9日 下午5:38:27
	 */
	private Long createSourcerowid(){
		synchronized (lock) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			return  NumberUtils.toLong(DateFormatUtils.format(System.currentTimeMillis(), MetaqConstants.DATE_FORMAT_PATTERN_17));
		}
	}
	
	/**
	 * 
	 * 选取作为时间转化的字段（转化为 年 月 日 时 分）
	 * @author shilei
	 * @date 2016年5月9日 上午10:28:07
	 * @param userOperaTionLogBean
	 */
	private void transOperaTionTime(UserOperaTionLogBean userOperaTionLogBean) {
		
		String operationDayField = PropertiesUtil.getValue(MetaqConstants.OPERATIONDAYFIELD,UOTLField.OPERATIONDT);
		try {
			String valueStr = BeanUtils.getProperty(userOperaTionLogBean, operationDayField);
			if(UOTLField.MS.equals(operationDayField) || StringUtils.isBlank(valueStr)){
				valueStr = BeanUtils.getProperty(userOperaTionLogBean, UOTLField.MS);
				valueStr = DateFormatUtils.format(NumberUtils.toLong(valueStr), MetaqConstants.DATE_FORMAT_PATTERN_14);
			}
			
			Long operationDayDt = NumberUtils.toLong(valueStr)/100;
			// 分钟分单位 201605271230
			userOperaTionLogBean.setOperationdaydt(operationDayDt);  
			// 十分钟为单位 20160527123
			userOperaTionLogBean.setOperationdayd(operationDayDt/10);  
			// 年 2016
			userOperaTionLogBean.setOperationyear(operationDayDt/(100*100*100*100));        
			// 月 5
			userOperaTionLogBean.setOperationmonth((operationDayDt/(100*100*100))%100); 
			// 天 27
			userOperaTionLogBean.setOperationday((operationDayDt/(100*100))%100);   
			// 小时 12
			userOperaTionLogBean.setOperationhour((operationDayDt/100)%100);
			// 年月 201605
			userOperaTionLogBean.setColumnField1(String.valueOf(operationDayDt/(100*100*100)));
			// 年月日 20160527
			userOperaTionLogBean.setColumnField2(String.valueOf(operationDayDt/(100*100)));
			// 年月日时 2016052712
			userOperaTionLogBean.setColumnField3(String.valueOf(operationDayDt/100));
		} catch (Exception e) {
			LOGGER.warn("userOperaTionLogBean get operationDayField : {} error", operationDayField);
		}	
	}
	
	public static void main(String[] args) {
		Long operationDayDt = 201605271230L;
		System.out.println(String.valueOf(operationDayDt/(100*100*100)));
		System.out.println(String.valueOf(operationDayDt/(100*100)));
		System.out.println(String.valueOf(operationDayDt/100));//1464312718388 1464312712156
		System.out.println(DateFormatUtils.format(NumberUtils.toLong("1464312718388"), MetaqConstants.DATE_FORMAT_PATTERN_14));
		String operationDayField = PropertiesUtil.getValue(MetaqConstants.OPERATIONDAYFIELD,UOTLField.OPERATIONDT);
		UserOperaTionLogBean userOperaTionLogBean = new UserOperaTionLogBean();
		userOperaTionLogBean.setMs(1464312718388L);
		String valueStr;
		try {
			valueStr = BeanUtils.getProperty(userOperaTionLogBean, operationDayField);
			if(UOTLField.MS.equals(operationDayField) || StringUtils.isBlank(valueStr)){
				valueStr = BeanUtils.getProperty(userOperaTionLogBean, UOTLField.MS);
				System.out.println("ms:"+valueStr);
				valueStr = DateFormatUtils.format(NumberUtils.toLong(valueStr), MetaqConstants.DATE_FORMAT_PATTERN_14);
			}
			System.out.println(valueStr);
			System.out.println(NumberUtils.toLong(valueStr)/100);
		} catch (Exception e) {
			e.printStackTrace();
			
		} 
		
	}
	
	/**
	 * 设置渠道类型和站定来源类型(Web,App,H5)
	 * @author shilei
	 * @date 2016年4月25日 上午10:08:02
	 * @param odsAsUOLVo
	 * @return
	 */
	public static void sourcesAndSiteType4Channel(UserOperaTionLogBean userOperaTionLogBean) {
		if(StringUtils.isBlank(userOperaTionLogBean.getChannel())){
			return ;
		}
		String channel = (String)userOperaTionLogBean.getChannel();
		String sourcesType = PropertiesUtil.getChannelType(channel);
		//设置通道类型
		userOperaTionLogBean.setSourcestype(sourcesType);
		
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
		userOperaTionLogBean.setSitetype(siteType);
	}
	
	/**
	 * 提取UTM字段
	 *
	 * @author shilei
	 * @date 2016年5月9日 上午10:24:35
	 * @param userOperaTionLogBean
	 */
	private void extractUtmField(UserOperaTionLogBean userOperaTionLogBean) {
		String utm_term = userOperaTionLogBean.getUtmTerm()==null?null:(String)userOperaTionLogBean.getUtmTerm();
		String utm_medium = userOperaTionLogBean.getUtmMedium()==null?null:(String)userOperaTionLogBean.getUtmMedium();
		String utm_source = userOperaTionLogBean.getUtmSource()==null?null:(String)userOperaTionLogBean.getUtmSource();
		//http://ship1.mangocity.com/cruise-line_2_0_0_0_0_0.html?utm_source=bdyoulun&utm_medium=cpc&utm_term=baidu
		String pvurl = userOperaTionLogBean.getPvurl()==null?null:(String)userOperaTionLogBean.getPvurl();
		String refer = userOperaTionLogBean.getRefer()==null?null:(String)userOperaTionLogBean.getRefer();
		// 取pvurl上的参数
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(pvurl);
		
		if(paramsMap==null || paramsMap.size()<3){
			// 取refer上的参数
			paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		}
		if(paramsMap!=null){
			if(paramsMap.containsKey(UOTLField.UTM_TERM) && StringUtils.isBlank(utm_term)){
				utm_term = paramsMap.get(UOTLField.UTM_TERM);
				// 对关键字进行解码操作
				if(StringUtils.isNoneBlank(utm_term)){
					try {
						URLDecoder.decode(utm_term, MetaqConstants.UTF_8);
					} catch (Exception e) {
						LOGGER.warn("decode fial utm_term:{}",utm_term, e);
					}
				}
			}
			if(paramsMap.containsKey(UOTLField.UTM_MEDIUM) && StringUtils.isBlank(utm_medium)){
				utm_medium = paramsMap.get(UOTLField.UTM_MEDIUM);
			}
			if(paramsMap.containsKey(UOTLField.UTM_SOURCE) && StringUtils.isBlank(utm_source)){
				utm_source = paramsMap.get(UOTLField.UTM_SOURCE);
			}
			if(paramsMap.containsKey(UOTLField.SID)){
				String sid = paramsMap.get(UOTLField.SID);
				userOperaTionLogBean.setSid(sid);
			}
		}
		
		userOperaTionLogBean.setUtmTerm(utm_term);
		userOperaTionLogBean.setUtmMedium(utm_medium);
		userOperaTionLogBean.setUtmSource(utm_source);
	}
	
	/**
	 * 将pvurl转化成url
	 * @param pvurl
	 * @return
	 */
	protected void pvUrlToUrl(UserOperaTionLogBean userOperaTionLogBean,String pvurl) {
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
				//url = pvurl.substring(0, pvurl.indexOf("?"));
				url = StringUtils.substring(pvurl, 0, pvurl.indexOf("?"));
			}else if(pvurl.contains("#")){// 截取#之前的url
				//url = pvurl.substring(0, pvurl.indexOf("#"));
				url = StringUtils.substring(pvurl, 0, pvurl.indexOf("#"));
			}else{
				//验证pvurl是不是非法pvurl
				pattern = Pattern.compile(PropertiesUtil.getValue(MetaqConstants.LEGAL_URL_REGEX));
				matcher = pattern.matcher(pvurl);
				flag = matcher.matches();
				if(!flag){//不是合法的pvurl
					pattern = Pattern.compile(PropertiesUtil.getValue(MetaqConstants.ILLEGALITY_URL_REGEX));
					matcher = pattern.matcher(pvurl);
					if(matcher.matches()){//不是合法的pvurl
						url = null;
					}
				}
			}
		}
		if(webPageType==MetaqConstants.DETAIL_PAGE_TYPE){//详情页面
			// 提取产品ID
			String productid = url.substring(url.indexOf("-")+1, url.indexOf(".html"));
			userOperaTionLogBean.setProjectid(productid);	
		}
		userOperaTionLogBean.setUrl(url);
		userOperaTionLogBean.setWebpagetypeid(webPageType);
	}
	

	
}

