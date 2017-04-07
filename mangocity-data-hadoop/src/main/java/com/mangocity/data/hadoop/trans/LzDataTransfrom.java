package com.mangocity.data.hadoop.trans;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import com.mangocity.data.commons.util.HttpClientUtil;
import com.mangocity.data.commons.util.PropertiesUtil;
import com.mangocity.data.hadoop.util.HadoopConstants;

/**
 *
 * @author shilei
 * @date 2016年6月30日 上午8:26:57 
 */
public class LzDataTransfrom extends DataTransfrom {
	
	private final static Set<Long> sourcerowidSet = new HashSet<Long>();
	private final static Map<String,String> PAGE_TITLE_MAP = new HashMap<String,String>();
	
	public GenericRecord handleData(GenericRecord record){
		if(record==null || record.get("sourcerowid")==null){
			return null;
		}
		Object rowid = record.get("sourcerowid");
		Long sourcerowid = NumberUtils.toLong(rowid.toString());
		if(sourcerowidSet.contains(sourcerowid)){
			return null;
		}else{
			sourcerowidSet.add(sourcerowid);
		}
		
		getTitle(record);
		
		getSid(record);
		
		Long operationdt = record.get("operationdt")==null?null:NumberUtils.toLong(record.get("operationdt").toString());
		Long operationdaydt = record.get("operationdaydt")==null?null:NumberUtils.toLong(record.get("operationdaydt").toString());//userOperaTionLogBean.getOperationdaydt();
		String columnField1 = record.get("columnField1")==null?null:record.get("columnField1").toString();//userOperaTionLogBean.getColumnField1();
		if(operationdt==null || (operationdaydt>0 && operationdaydt/100000000==1970) || StringUtils.isBlank(columnField1)){
			optionTime(record);
		}
		sourcesAndSiteType4Channel(record);
			
		return record;
	}
	
	private void getTitle(GenericRecord record) {
		Object pvurl = record.get("pvurl");
		if(pvurl==null){
			return ;
		}
		Object title = record.get("title");
		if(title == null || StringUtils.isBlank(title.toString())){
			try {
				Thread.sleep(1);
			} catch (Exception e) {
				e.printStackTrace();
			}
			title = getPageTitle(pvurl.toString());
			if(title!=null){
				record.put("title",title);
			}else{
				System.out.println("sourcerowid:"+record.get("sourcerowid")+",pvurl:"+record.get("pvurl")+", title is null");
			}
		}else if(!PAGE_TITLE_MAP.containsKey(title)){
			PAGE_TITLE_MAP.put(pvurl.toString(), title.toString());
		}
	}

	private void getSid(GenericRecord record) {
		Object referObj = record.get("refer");
		if(referObj == null){
			return ;
		}
		String refer = referObj.toString();
		Map<String,String> paramsMap = HttpClientUtil.httpUrlParserParams(refer);
		if(paramsMap!=null){
			if(paramsMap.containsKey("sid") && record.get("sid")==null){
				String sid = paramsMap.get("sid");
				record.put("sid",sid);
			}
			if(paramsMap.containsKey("uid") && record.get("suid")==null){
				String suidStr = paramsMap.get("uid");
				Integer suid = NumberUtils.toInt(suidStr)==0?null:NumberUtils.toInt(suidStr);
				record.put("suid",suid);
			}
		}
	}


	private void optionTime(GenericRecord record){
		Long ms = record.get("ms")==null?null:NumberUtils.toLong(record.get("ms").toString());
		if(ms==null || ms<=0){
			return;
		}
		String operationdtStr = DateFormatUtils.format(ms, HadoopConstants.DATE_FORMAT_PATTERN_14);
		Long operationDayDt = NumberUtils.toLong(operationdtStr)/100;
		// 分钟分单位 201605271230
		record.put("operationdaydt",operationDayDt);
		//userOperaTionLogBean.setOperationdaydt(operationDayDt);  
		// 十分钟为单位 20160527123
		record.put("operationdayd",operationDayDt/10);
		// 小时 12
		record.put("operationhour",(operationDayDt/100)%100);
		//userOperaTionLogBean.setOperationhour((operationDayDt/100)%100);
		// 年月 201605
		record.put("column_field1",String.valueOf(operationDayDt/(100*100*100)));
		//userOperaTionLogBean.setColumnField1(String.valueOf(operationDayDt/(100*100*100)));
		// 年月日 20160527
		record.put("column_field2",String.valueOf(operationDayDt/(100*100)));
		//userOperaTionLogBean.setColumnField2(String.valueOf(operationDayDt/(100*100)));
		// 年月日时 2016052712
		record.put("column_field3",String.valueOf(operationDayDt/100));
		//userOperaTionLogBean.setColumnField3(String.valueOf(operationDayDt/100));
	}
	
	/**
	 * 设置渠道类型和站定来源类型(Web,App,H5)
	 * @author shilei
	 * @date 2016年4月25日 上午10:08:02
	 * @param odsAsUOLVo
	 * @return
	 */
	public void sourcesAndSiteType4Channel(GenericRecord record) {
		Object channelObj = record.get("channel");
		if(channelObj==null || StringUtils.isBlank(channelObj.toString())){
			return ;
		}
		String channel = channelObj.toString();
		String sourcestype = getStringValue(record, "sourcestype");
		if(StringUtils.isNotBlank(sourcestype)){
			String sourcesType = PropertiesUtil.getChannelType(channel);
			//设置通道类型
			putValue(record, "sourcestype", sourcesType);
		}
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
		putValue(record, "sitetype", siteType);
	}
	

	/**
	 * 获取页面的标题
	 * @param url
	 * @return
	 */
	protected String getPageTitle(CharSequence urlc){
		if(StringUtils.isBlank(urlc)){
			return null;
		}
		String url = urlc.toString();
		String pageTile = null;
		if(PAGE_TITLE_MAP.containsKey(url)){
			pageTile = PAGE_TITLE_MAP.get(url);
		}else if(StringUtils.isNotBlank(url)){
			//pageTile = VfsUtils.readFileToString(url);
			//pageTile = getPageTitle(pageTile);
			pageTile = HttpClientUtil.getPageTitle(url);
			if(StringUtils.isNotBlank(pageTile)){
				PAGE_TITLE_MAP.put(url, pageTile);
			}
		}
		return pageTile;
	}
	
	
	

}

