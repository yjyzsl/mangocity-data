//package com.mangocity.data.commons;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.commons.io.FileUtils;
//import org.apache.commons.io.IOUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.jboss.netty.util.internal.ConcurrentHashMap;
//
//import com.alibaba.fastjson.JSONObject;
//import com.mangocity.data.commons.util.HttpClientUtil;
//
///**
// *
// * @author shilei
// * @date 2016年6月28日 下午2:01:57 
// */
//public class Tools {
//	
//	private static Map<String,String> pvMap = new ConcurrentHashMap<String,String>();
//	
//	public static void main(String[] args) {
//		
//		try (InputStream input = Tools.class.getClassLoader().getResourceAsStream("pv_info.txt")){
//			List<String> pvList = IOUtils.readLines(input,"UTF-8");
//			System.out.println(pvList.size());
//			String title = null;
//			int count = 0 ;
//			int count2 = 0;
//			long totalCount = 0L;
//			for (String pv : pvList) {
//				//title = HttpClientUtil.getPageTitle(pv);
//				StringUtils.split(pv, ",");
//				if(StringUtils.isNotBlank(title)){
//					pvMap.put(pv, title);
//				}else{
//					System.out.println("totalCount:"+totalCount+"map size:"+pvMap.size()+",pv:"+pv);
//				}
//				count++;
//				if(count%20 == 0){
//					JSONObject jsonObject = new JSONObject();
//					jsonObject.putAll(pvMap);
//					FileUtils.writeStringToFile(new File("pvMap.txt"), jsonObject.toString()+"\n", "UTF-8", true);
//					totalCount += pvMap.size();
//					pvMap.clear();
//					jsonObject.clear();
//					
//					count = 0;
//				}
//				count2++;
//				if(count2%3==0){
//					Thread.sleep(200);
//					count2 = 0;
//				}
//			}
//			System.out.println("totalCount:"+totalCount);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//
//}
//
