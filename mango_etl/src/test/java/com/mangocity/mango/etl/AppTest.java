package com.mangocity.mango.etl;

import java.sql.Date;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.DateFormatUtils;

import com.mangocity.etl.util.Constants;
import com.mangocity.etl.util.PropertiesUtil;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    
    public void testDate(){
    	System.out.println(DateFormatUtils.format(new Date(System.currentTimeMillis()), "yyyyMMddhhmmss SSS"));
    	
    	
    }
    
    
    public static void main(String[] args) {
//		String url = "http://ship1.mangocity.com/0-16081.html";
//		String regex = "http://ship1\\.mangocity\\.com/\\d-\\d+\\.html";
    	
//    	String url = "http://ship1.mangocity.com/ship-order-item.shtml";
//		String regex = "http://ship1\\.mangocity\\.com/\\w+-order-item\\.shtml";
	
		
		
//		String url = "http://ship1.mangocity.com/ship-order-complete.shtml";
//		String regex = "http://ship1\\.mangocity\\.com/ship-\\w+-complete\\.shtml";

//		String url = "http://ship1.mangocity.com/cruise-line_2_0_0_0_0_0.html?utm_source=bdyoulun&utm_medium=cpc&utm_term=baidu";
//		String regex = "http://ship1\\.mangocity\\.com/cruise-line_.*";
		//{m.mangocity.*=H5, app.mangocity.fit=App, \w*\.mangocity\.com"=Web, \w*market\w*=App}
//    	String url = "http://www.mangocity.com/search/";
//    	String url = "http://www.mangocity.com/安宁买大麻②⒈1⒉3Б8⒐扣号/package/";
    	String url = "http://lvyou.mangocity.com/F222012.html";
//    	String url = "http://www.mangocity.com/product/10557882p2.html";
//		String regex = "http://www.mangocity.com/w/.*\\W+.*";
		String regex = "http://www.mangocity.com/[\\w+/]*[.*html]*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(url);
		boolean flag = matcher.matches();
		System.out.println(flag);
//		
//		String ab = "abc+$";
//		String a = "a";
//		String b = "bc+$";
//		String c = a+b;
//		StringBuffer d = new StringBuffer();
//		HashSet<Integer> set = new HashSet<Integer>();
//		set.add(ab.hashCode());
//		System.out.println(set.contains(c.hashCode()));
//		System.out.println(set.contains(d.append(a).append(b).toString().hashCode()));
//		System.out.println(set.contains(a.hashCode()));
//		System.out.println(ab.hashCode());
//		System.out.println(c.hashCode());
//		System.out.println(d.append(a).append(b).toString().hashCode());
//		System.out.println(ab.hashCode()==c.hashCode());
		
		
		
		
	}
}
