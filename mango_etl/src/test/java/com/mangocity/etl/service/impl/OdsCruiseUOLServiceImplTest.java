package com.mangocity.etl.service.impl;

import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.mangocity.etl.service.OdsCruiseUOLService;

public class OdsCruiseUOLServiceImplTest {

	private ApplicationContext context;
	private OdsCruiseUOLService odsCruiseUOLService;
	@Before
	public void before(){
		context = new ClassPathXmlApplicationContext("classpath:spring-mybatis.xml");
		odsCruiseUOLService = (OdsCruiseUOLService) context.getBean("odsCruiseUOLService");
		
	}
	
	@Test
	public void testFindMaxSourceRowId() {
		System.out.println(odsCruiseUOLService.findMaxSourceRowId());
	}

}
