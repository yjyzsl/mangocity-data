package com.mangocity.etl.util;

import org.apache.log4j.Logger;
import org.junit.Test;

public class HttpClientUtilTest {

	private final static Logger LOGGER = Logger.getLogger(HttpClientUtilTest.class);
	
	@Test
	public void testHttpGetRequestString() {
		String url = "http://ship1.mangocity.com/0-15621.html";
		String result = HttpClientUtil.getPageTitle(url);
		System.out.println(result);
		LOGGER.info("@@@@@@@@:"+result);
	}

}
