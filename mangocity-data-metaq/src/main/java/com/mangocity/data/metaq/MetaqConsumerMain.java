package com.mangocity.data.metaq;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 * @author shilei
 * @date 2016年5月9日 下午3:05:29 
 */
public class MetaqConsumerMain {

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("classpath*:conf/metaq-consumer-beans.xml");
	}
	
}

