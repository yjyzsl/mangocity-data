package com.mangocity.etl.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.fastjson.JSON;
import com.mangocity.etl.service.UserOperaTionLogService;
import com.mangocity.etl.vo.UserOperaTionLogVo;

public class UserOperaTionLogServiceImplTest {

	private ApplicationContext context;
	private UserOperaTionLogService userOperaTionLogService;
	@Before
	public void before(){
		context = new ClassPathXmlApplicationContext("classpath:spring-mybatis.xml");
		userOperaTionLogService = (UserOperaTionLogService) context.getBean("userOperaTionLogService");
	}
	
	@Test
	public void testFindMaxSourceRowIdByChannel(){
		UserOperaTionLogVo vo  = new UserOperaTionLogVo();
		vo.setChannel("ship1.mangocity.com");
		System.out.println(userOperaTionLogService.findMaxSourceRowIdByChannel(vo));
	}
	
	
	@Test
	public void testFindCount(){
		UserOperaTionLogVo vo  = new UserOperaTionLogVo();
		vo.setChannel("ship1.mangocity.com");
//		vo.setSourcerowid(16221907);
		vo.setSourcerowid(-1);
		System.out.println(userOperaTionLogService.findCount(vo));
	}
	
	@Test
	public void testFindPageBySourcerowid(){
		Map<String,Object> params = new HashMap<String, Object>();
		//params.put("channel", "ship1.mangocity.com");
		params.put("currentRowId", 91055956);
		params.put("nextRowId", 91056056);
		List<UserOperaTionLogVo> list =  userOperaTionLogService.findPageBySourcerowid(params);
		System.out.println(list.size());
		String jsonStr = JSON.toJSONString(list);
		try {
			FileUtils.write(new File("useroperationlogvo_29.json"), jsonStr);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testFindPageByRowNum(){
		Map<String,Object> params = new HashMap<String, Object>();
		params.put("channel", "ship1.mangocity.com");
		params.put("currentRowId", 0);
		params.put("nextRowId", 30);
		System.out.println(userOperaTionLogService.findPageByRowNum(params).size());
	}

	
	public static void main(String[] args) {
		System.out.println("-----------------------");
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath:spring-mybatis.xml");
		UserOperaTionLogService userOperaTionLogService = (UserOperaTionLogService) context.getBean("userOperaTionLogService");
//		UserOperaTionLogVo vo = userOperaTionLogService.findById(81945624);
//		System.out.println(vo);
//		System.out.println(DateFormatUtils.format(vo.getSourcerowcreatedt(), "yyyyMMdd HHmmss"));
		
		
//		System.out.println(userOperaTionLogService.findById(5877012));
//		List<UserOperaTionLogVo> userOperaTionLogVos = userOperaTionLogService.findAll();
//		for (UserOperaTionLogVo userOperaTionLogVo : userOperaTionLogVos) {
//			System.out.println(userOperaTionLogVo);
//		}
		
	}
}
