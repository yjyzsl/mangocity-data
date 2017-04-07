package com.mangocity.etl.util;

import org.junit.Test;

public class HttpUtilTest {

	@Test
	public void testHttpUrlParserParams() {
		String url = "http://www.mangocity.com/index.php/cpscontroller/cpsindex?cps=cps&projectcode=0020001&url=http://ship1.mangocity.com/cruise-";
		System.out.println(HttpClientUtil.httpUrlParserParams(url));
		url = "http://ship1.mangocity.com/cruise-line_2_0_0_0_0_0.html?utm_source=bdyoulun&utm_medium=cpc&utm_term=baidu";
		System.out.println(HttpClientUtil.httpUrlParserParams(url));
	}

}
