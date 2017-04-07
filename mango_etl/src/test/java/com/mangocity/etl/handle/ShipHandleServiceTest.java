package com.mangocity.etl.handle;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ShipHandleServiceTest {

	@Test
	public void testHandle() {
		fail("Not yet implemented");
	}
	
	public static void main(String[] args) {
		System.out.println("-----------------------");
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath:spring-mybatis.xml");
		HandleService handleService = (HandleService) context.getBean("handleService");
		handleService.handle(123);
	}

}
