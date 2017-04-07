package com.mangocity.etl.vo;

import java.util.Date;

/**
 * 用户操作日志表
 * @author shilei
 *
 */
public class UserOperaTionLogVo {
	
	private  Integer sourcerowid;
	private  String  channel;
	private  Integer operationtype;
	private  String  mbrid;
	private  String  sessionid;
	private  String  mangouid;
	private  String  pvurl;
	private  String  ipaddr;
	private  String  cityname;
	private  String  postcode;
	private  Date    operationdt;
	private  String  language;
	private  String  browermodel;
	private  String  systemmodel;
	private  String  screenresolution;
	private  String  businesschannel;
	private  String  searchinpumethod;
	private  String  searchinputtype;
	private  String  searchkeywords;
	private  String  reqparam;
	private  String  searchpath;
	private  Date    searchstarttime;
	private  Date    searchendtime;
	private  String  searchresnum;
	private  String  searchresstatus;
	private  String  goodsnumber;
	private  String  ordernumber;
	private  String  orderserviceline;
	private  String  ordertotal;
	private  String  clickid;
	private  String  clickhref;
	private  String  reqsourcetype;
	private  String  currentpagearea;
	private  String  refer;
	private  String  refsubdomain;
	private  String  refmaindomain;
	private  String  sid;
	private  String  searchword;
	private  String  camid;
	private  String  adgid;
	private  String  kwid;
	private  String  utm_term;
	private  String  ctvid;
	private  String  utm_content;
	private  String  place;
	private  String  utm_source;
	private  String  utm_medium;
	private  String  landon_url;
	private  String  usercookieall;
	private  String  pvurlsubdomain;
	private  String  clickhrefbase;
	private  String  productid;
	private  Date    sourcerowcreatedt;
	private  String  pagename;
	private  String  viewid;
	private  String  viewname;
	private  String  machinetype;
	private  String  systemversionnum;
	private  String  operationdescription;
	private  String  longitude;
	private  String  latitude;
	private  Integer webpagetypeid;
	private  String  softwareversion;
	private  String  deviceid;
	private  String  devicebrand;
	private  String  ordercreationstatus;
	private  String  ordertype;
	private  String  localphoneIntger;
	private  String  eventviewtype;
	private  String  eventtype;
	private  String  eventname;
	private  String  eventpagename;
	private  String  eventmodule;
	private  Integer operationday;
	private  Integer etl_load_tag;
	private  Date 	 etl_insert_dt;
	private  Date    etl_update_dt;
	
	// add by shilei 20160505
	private  Integer still_time;//页面的停留时间
	private  Integer focus_time;//焦点时间
	private  String  user_agent;//浏览器UA
	private  String  color_depth;//颜色深度
	private  Long    ms;//dsp接收数据的时间
	private  String  tile;//标题
	private  Long    operation_ms;//客户端操作时间的毫秒表示：同一个页面，1~7种事件类型（click除外）的operation_ms值是一样
	
	public Integer getSourcerowid() {
		return sourcerowid;
	}
	public void setSourcerowid(Integer sourcerowid) {
		this.sourcerowid = sourcerowid;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public Integer getOperationtype() {
		return operationtype;
	}
	public void setOperationtype(Integer operationtype) {
		this.operationtype = operationtype;
	}
	public String getMbrid() {
		return mbrid;
	}
	public void setMbrid(String mbrid) {
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
	public String getPvurl() {
		return pvurl;
	}
	public void setPvurl(String pvurl) {
		this.pvurl = pvurl;
	}
	public String getIpaddr() {
		return ipaddr;
	}
	public void setIpaddr(String ipaddr) {
		this.ipaddr = ipaddr;
	}
	public String getCityname() {
		return cityname;
	}
	public void setCityname(String cityname) {
		this.cityname = cityname;
	}
	public String getPostcode() {
		return postcode;
	}
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}
	public Date getOperationdt() {
		return operationdt;
	}
	public void setOperationdt(Date operationdt) {
		this.operationdt = operationdt;
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
	public String getScreenresolution() {
		return screenresolution;
	}
	public void setScreenresolution(String screenresolution) {
		this.screenresolution = screenresolution;
	}
	public String getBusinesschannel() {
		return businesschannel;
	}
	public void setBusinesschannel(String businesschannel) {
		this.businesschannel = businesschannel;
	}
	public String getSearchinpumethod() {
		return searchinpumethod;
	}
	public void setSearchinpumethod(String searchinpumethod) {
		this.searchinpumethod = searchinpumethod;
	}
	public String getSearchinputtype() {
		return searchinputtype;
	}
	public void setSearchinputtype(String searchinputtype) {
		this.searchinputtype = searchinputtype;
	}
	public String getSearchkeywords() {
		return searchkeywords;
	}
	public void setSearchkeywords(String searchkeywords) {
		this.searchkeywords = searchkeywords;
	}
	public String getReqparam() {
		return reqparam;
	}
	public void setReqparam(String reqparam) {
		this.reqparam = reqparam;
	}
	public String getSearchpath() {
		return searchpath;
	}
	public void setSearchpath(String searchpath) {
		this.searchpath = searchpath;
	}
	public Date getSearchstarttime() {
		return searchstarttime;
	}
	public void setSearchstarttime(Date searchstarttime) {
		this.searchstarttime = searchstarttime;
	}
	public Date getSearchendtime() {
		return searchendtime;
	}
	public void setSearchendtime(Date searchendtime) {
		this.searchendtime = searchendtime;
	}
	public String getSearchresnum() {
		return searchresnum;
	}
	public void setSearchresnum(String searchresnum) {
		this.searchresnum = searchresnum;
	}
	public String getSearchresstatus() {
		return searchresstatus;
	}
	public void setSearchresstatus(String searchresstatus) {
		this.searchresstatus = searchresstatus;
	}
	
	public String getGoodsnumber() {
		return goodsnumber;
	}
	public void setGoodsnumber(String goodsnumber) {
		this.goodsnumber = goodsnumber;
	}
	public String getOrdernumber() {
		return ordernumber;
	}
	public void setOrdernumber(String ordernumber) {
		this.ordernumber = ordernumber;
	}
	public String getOrderserviceline() {
		return orderserviceline;
	}
	public void setOrderserviceline(String orderserviceline) {
		this.orderserviceline = orderserviceline;
	}
	public String getOrdertotal() {
		return ordertotal;
	}
	public void setOrdertotal(String ordertotal) {
		this.ordertotal = ordertotal;
	}
	public String getClickid() {
		return clickid;
	}
	public void setClickid(String clickid) {
		this.clickid = clickid;
	}
	public String getClickhref() {
		return clickhref;
	}
	public void setClickhref(String clickhref) {
		this.clickhref = clickhref;
	}
	public String getReqsourcetype() {
		return reqsourcetype;
	}
	public void setReqsourcetype(String reqsourcetype) {
		this.reqsourcetype = reqsourcetype;
	}
	public String getCurrentpagearea() {
		return currentpagearea;
	}
	public void setCurrentpagearea(String currentpagearea) {
		this.currentpagearea = currentpagearea;
	}
	public String getRefer() {
		return refer;
	}
	public void setRefer(String refer) {
		this.refer = refer;
	}
	public String getRefsubdomain() {
		return refsubdomain;
	}
	public void setRefsubdomain(String refsubdomain) {
		this.refsubdomain = refsubdomain;
	}
	public String getRefmaindomain() {
		return refmaindomain;
	}
	public void setRefmaindomain(String refmaindomain) {
		this.refmaindomain = refmaindomain;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
	public String getSearchword() {
		return searchword;
	}
	public void setSearchword(String searchword) {
		this.searchword = searchword;
	}
	public String getCamid() {
		return camid;
	}
	public void setCamid(String camid) {
		this.camid = camid;
	}
	public String getAdgid() {
		return adgid;
	}
	public void setAdgid(String adgid) {
		this.adgid = adgid;
	}
	public String getKwid() {
		return kwid;
	}
	public void setKwid(String kwid) {
		this.kwid = kwid;
	}
	public String getUtm_term() {
		return utm_term;
	}
	public void setUtm_term(String utm_term) {
		this.utm_term = utm_term;
	}
	public String getCtvid() {
		return ctvid;
	}
	public void setCtvid(String ctvid) {
		this.ctvid = ctvid;
	}
	public String getUtm_content() {
		return utm_content;
	}
	public void setUtm_content(String utm_content) {
		this.utm_content = utm_content;
	}
	public String getPlace() {
		return place;
	}
	public void setPlace(String place) {
		this.place = place;
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
	public String getLandon_url() {
		return landon_url;
	}
	public void setLandon_url(String landon_url) {
		this.landon_url = landon_url;
	}
	public String getUsercookieall() {
		return usercookieall;
	}
	public void setUsercookieall(String usercookieall) {
		this.usercookieall = usercookieall;
	}
	public String getPvurlsubdomain() {
		return pvurlsubdomain;
	}
	public void setPvurlsubdomain(String pvurlsubdomain) {
		this.pvurlsubdomain = pvurlsubdomain;
	}
	public String getClickhrefbase() {
		return clickhrefbase;
	}
	public void setClickhrefbase(String clickhrefbase) {
		this.clickhrefbase = clickhrefbase;
	}
	public String getProductid() {
		return productid;
	}
	public void setProductid(String productid) {
		this.productid = productid;
	}
	public Date getSourcerowcreatedt() {
		return sourcerowcreatedt;
	}
	public void setSourcerowcreatedt(Date sourcerowcreatedt) {
		this.sourcerowcreatedt = sourcerowcreatedt;
	}
	public String getPagename() {
		return pagename;
	}
	public void setPagename(String pagename) {
		this.pagename = pagename;
	}
	public String getViewid() {
		return viewid;
	}
	public void setViewid(String viewid) {
		this.viewid = viewid;
	}
	public String getViewname() {
		return viewname;
	}
	public void setViewname(String viewname) {
		this.viewname = viewname;
	}
	public String getMachinetype() {
		return machinetype;
	}
	public void setMachinetype(String machinetype) {
		this.machinetype = machinetype;
	}
	public String getSystemversionnum() {
		return systemversionnum;
	}
	public void setSystemversionnum(String systemversionnum) {
		this.systemversionnum = systemversionnum;
	}
	public String getOperationdescription() {
		return operationdescription;
	}
	public void setOperationdescription(String operationdescription) {
		this.operationdescription = operationdescription;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public Integer getWebpagetypeid() {
		return webpagetypeid;
	}
	public void setWebpagetypeid(Integer webpagetypeid) {
		this.webpagetypeid = webpagetypeid;
	}
	public String getSoftwareversion() {
		return softwareversion;
	}
	public void setSoftwareversion(String softwareversion) {
		this.softwareversion = softwareversion;
	}
	public String getDeviceid() {
		return deviceid;
	}
	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}
	public String getDevicebrand() {
		return devicebrand;
	}
	public void setDevicebrand(String devicebrand) {
		this.devicebrand = devicebrand;
	}
	public String getOrdercreationstatus() {
		return ordercreationstatus;
	}
	public void setOrdercreationstatus(String ordercreationstatus) {
		this.ordercreationstatus = ordercreationstatus;
	}
	public String getOrdertype() {
		return ordertype;
	}
	public void setOrdertype(String ordertype) {
		this.ordertype = ordertype;
	}
	public String getLocalphoneIntger() {
		return localphoneIntger;
	}
	public void setLocalphoneIntger(String localphoneIntger) {
		this.localphoneIntger = localphoneIntger;
	}
	public String getEventviewtype() {
		return eventviewtype;
	}
	public void setEventviewtype(String eventviewtype) {
		this.eventviewtype = eventviewtype;
	}
	public String getEventtype() {
		return eventtype;
	}
	public void setEventtype(String eventtype) {
		this.eventtype = eventtype;
	}
	public String getEventname() {
		return eventname;
	}
	public void setEventname(String eventname) {
		this.eventname = eventname;
	}
	public String getEventpagename() {
		return eventpagename;
	}
	public void setEventpagename(String eventpagename) {
		this.eventpagename = eventpagename;
	}
	public String getEventmodule() {
		return eventmodule;
	}
	public void setEventmodule(String eventmodule) {
		this.eventmodule = eventmodule;
	}
	public Integer getOperationday() {
		return operationday;
	}
	public void setOperationday(Integer operationday) {
		this.operationday = operationday;
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
	public String getTile() {
		return tile;
	}
	public void setTile(String tile) {
		this.tile = tile;
	}
	public Long getOperation_ms() {
		return operation_ms;
	}
	public void setOperation_ms(Long operation_ms) {
		this.operation_ms = operation_ms;
	}
	@Override
	public String toString() {
		return "UserOperaTionLogVo [sourcerowid=" + sourcerowid + ", channel="
				+ channel + ", operationtype=" + operationtype + ", mbrid="
				+ mbrid + ", pvurl=" + pvurl + ", cityname=" + cityname
				+ ", refer=" + refer + ", utm_term=" + utm_term
				+ ", utm_content=" + utm_content + ", utm_source=" + utm_source
				+ ", utm_medium=" + utm_medium + ", operationday="
				+ operationday + ", still_time=" + still_time + ", focus_time="
				+ focus_time + ", user_agent=" + user_agent + ", color_depth="
				+ color_depth + ", ms=" + ms + ", tile=" + tile + "]";
	}
	
	
	

}
