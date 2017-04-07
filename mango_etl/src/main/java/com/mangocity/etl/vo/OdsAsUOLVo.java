package com.mangocity.etl.vo;

import java.util.Date;

/**
 * 全部渠道
 * @author shilei
 *
 */
public class OdsAsUOLVo {
	
	private  Integer sourceRowId;
	private  Date    userOperationDt;   
	private  Integer userOperationTypeId;   
	private  Integer webpagetypeid;   
	private  Integer mbrid;   
	private  String  sessionid;
	private  String  mangouid;
	private  String  channel;
	// 渠道名称
	private  String  sourcesType;
	// 站点来源
	private  String  siteType;
	private  String  pvurl;
	private  String  url;
	private  String  pageTitle;
	private  String  ip;
	private  String  cityname;
	private  String  language;
	private  String  browermodel;
	private  String  systemmodel;
	private  String  machinetype;
	private  String  searchkeywords;
	private  String  refer;
	private  String  sdtRefer;
	private  Integer sid;   
	private  Integer suid;	   
	private  String  projectID;   
	private  String  searchword;
	private  String  utm_term;
	private  String  utm_source;
	private  String  utm_medium;
	private  String  productid;
	private  String  orderCd;
	private  Date    sourceRowCreateDt;   
	private  Long operationDayDt; 
	// 十分钟为单位 
	private  Long operationDayD;
	private  Long operationYear;   
	private  Long operationMonth;   
	private  Long operationDay;   
	private  Long operationHour;   
	private  Integer etl_load_tag;
	private  Date    etl_insert_dt;   
	private  Date    etl_update_dt;
	private  Integer still_time;//页面的停留时间
	private  Integer focus_time;//焦点时间
	private  String  user_agent;//浏览器UA
	private  String  color_depth;//颜色深度
	private  Long    ms;//dsp接收数据的时间
	private  Long    operation_ms;//客户端操作时间的毫秒表示：同一个页面，1~7种事件类型（click除外）的operation_ms值是一样
	
	
	public Integer getSourceRowId() {
		return sourceRowId;
	}
	public void setSourceRowId(Integer sourceRowId) {
		this.sourceRowId = sourceRowId;
	}
	public Date getUserOperationDt() {
		return userOperationDt;
	}
	public void setUserOperationDt(Date userOperationDt) {
		this.userOperationDt = userOperationDt;
	}
	public Integer getUserOperationTypeId() {
		return userOperationTypeId;
	}
	public void setUserOperationTypeId(Integer userOperationTypeId) {
		this.userOperationTypeId = userOperationTypeId;
	}
	public Integer getWebpagetypeid() {
		return webpagetypeid;
	}
	public void setWebpagetypeid(Integer webpagetypeid) {
		this.webpagetypeid = webpagetypeid;
	}
	public Integer getMbrid() {
		return mbrid;
	}
	public void setMbrid(Integer mbrid) {
		this.mbrid = mbrid;
	}
	public String getSessionid() {
		return sessionid;
	}
	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}
	public String getMangouid() {
		return mangouid;
	}
	public void setMangouid(String mangouid) {
		this.mangouid = mangouid;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public String getSourcesType() {
		return sourcesType;
	}
	public void setSourcesType(String sourcesType) {
		this.sourcesType = sourcesType;
	}
	public String getSiteType() {
		return siteType;
	}
	public void setSiteType(String siteType) {
		this.siteType = siteType;
	}
	public String getPvurl() {
		return pvurl;
	}
	public void setPvurl(String pvurl) {
		this.pvurl = pvurl;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getCityname() {
		return cityname;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public String getBrowermodel() {
		return browermodel;
	}
	public void setBrowermodel(String browermodel) {
		this.browermodel = browermodel;
	}
	public String getSystemmodel() {
		return systemmodel;
	}
	public void setSystemmodel(String systemmodel) {
		this.systemmodel = systemmodel;
	}
	public String getMachinetype() {
		return machinetype;
	}
	public void setMachinetype(String machinetype) {
		this.machinetype = machinetype;
	}
	public String getSearchkeywords() {
		return searchkeywords;
	}
	public void setSearchkeywords(String searchkeywords) {
		this.searchkeywords = searchkeywords;
	}
	public String getRefer() {
		return refer;
	}
	public void setRefer(String refer) {
		this.refer = refer;
	}
	
	public String getSdtRefer() {
		return sdtRefer;
	}
	public void setSdtRefer(String sdtRefer) {
		this.sdtRefer = sdtRefer;
	}
	public Integer getSid() {
		return sid;
	}
	public void setSid(Integer sid) {
		this.sid = sid;
	}
	
	public String getPageTitle() {
		return pageTitle;
	}
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}
	public Integer getSuid() {
		return suid;
	}
	public void setSuid(Integer suid) {
		this.suid = suid;
	}
	public String getProjectID() {
		return projectID;
	}
	public void setProjectID(String projectID) {
		this.projectID = projectID;
	}
	public String getSearchword() {
		return searchword;
	}
	public void setSearchword(String searchword) {
		this.searchword = searchword;
	}
	public String getUtm_term() {
		return utm_term;
	}
	public void setUtm_term(String utm_term) {
		this.utm_term = utm_term;
	}
	public String getUtm_source() {
		return utm_source;
	}
	public void setUtm_source(String utm_source) {
		this.utm_source = utm_source;
	}
	public String getUtm_medium() {
		return utm_medium;
	}
	public void setUtm_medium(String utm_medium) {
		this.utm_medium = utm_medium;
	}
	public String getProductid() {
		return productid;
	}
	public void setProductid(String productid) {
		this.productid = productid;
	}
	public String getOrderCd() {
		return orderCd;
	}
	public void setOrderCd(String orderCd) {
		this.orderCd = orderCd;
	}
	public Date getSourceRowCreateDt() {
		return sourceRowCreateDt;
	}
	public void setSourceRowCreateDt(Date sourceRowCreateDt) {
		this.sourceRowCreateDt = sourceRowCreateDt;
	}
	
	public Long getOperationDayDt() {
		return operationDayDt;
	}
	public void setOperationDayDt(Long operationDayDt) {
		this.operationDayDt = operationDayDt;
	}
	public Long getOperationDayD() {
		return operationDayD;
	}
	public void setOperationDayD(Long operationDayD) {
		this.operationDayD = operationDayD;
	}
	public Long getOperationYear() {
		return operationYear;
	}
	public void setOperationYear(Long operationYear) {
		this.operationYear = operationYear;
	}
	public Long getOperationMonth() {
		return operationMonth;
	}
	public void setOperationMonth(Long operationMonth) {
		this.operationMonth = operationMonth;
	}
	public Long getOperationDay() {
		return operationDay;
	}
	public void setOperationDay(Long operationDay) {
		this.operationDay = operationDay;
	}
	public Long getOperationHour() {
		return operationHour;
	}
	public void setOperationHour(Long operationHour) {
		this.operationHour = operationHour;
	}
	
	public Integer getEtl_load_tag() {
		return etl_load_tag;
	}
	public void setEtl_load_tag(Integer etl_load_tag) {
		this.etl_load_tag = etl_load_tag;
	}
	public Date getEtl_insert_dt() {
		return etl_insert_dt;
	}
	public void setEtl_insert_dt(Date etl_insert_dt) {
		this.etl_insert_dt = etl_insert_dt;
	}
	public Date getEtl_update_dt() {
		return etl_update_dt;
	}
	public void setEtl_update_dt(Date etl_update_dt) {
		this.etl_update_dt = etl_update_dt;
	}
	
	public Integer getStill_time() {
		return still_time;
	}
	public void setStill_time(Integer still_time) {
		this.still_time = still_time;
	}
	public Integer getFocus_time() {
		return focus_time;
	}
	public void setFocus_time(Integer focus_time) {
		this.focus_time = focus_time;
	}
	
	
	public String getUser_agent() {
		return user_agent;
	}
	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}
	public String getColor_depth() {
		return color_depth;
	}
	public void setColor_depth(String color_depth) {
		this.color_depth = color_depth;
	}
	public Long getMs() {
		return ms;
	}
	public void setMs(Long ms) {
		this.ms = ms;
	}
	public Long getOperation_ms() {
		return operation_ms;
	}
	public void setOperation_ms(Long operation_ms) {
		this.operation_ms = operation_ms;
	}
	@Override
	public String toString() {
		return "OdsAsUOLVo [sourceRowId=" + sourceRowId + ", userOperationDt="
				+ userOperationDt + ", userOperationTypeId="
				+ userOperationTypeId + ", webpagetypeid=" + webpagetypeid
				+ ", mbrid=" + mbrid + ", channel=" + channel
				+ ", sourcesType=" + sourcesType + ", siteType=" + siteType
				+ ", pvurl=" + pvurl + ", url=" + url + ", pageTitle="
				+ pageTitle + ", utm_term=" + utm_term + ", utm_source="
				+ utm_source + ", utm_medium=" + utm_medium
				+ ", sourceRowCreateDt=" + sourceRowCreateDt + ", still_time="
				+ still_time + ", focus_time=" + focus_time + "]";
	}
	
	
}

