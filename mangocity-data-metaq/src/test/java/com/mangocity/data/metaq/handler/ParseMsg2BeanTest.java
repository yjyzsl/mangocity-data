package com.mangocity.data.metaq.handler;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;
import com.mangocity.data.commons.bean.UserOperaTionLogBean;
import com.mangocity.data.metaq.handler.impl.UOALConsumerDataServiceImpl;

/**
 *
 * @author shilei
 * @date 2016年5月19日 下午8:10:49 
 */
public class ParseMsg2BeanTest {

	@Test
	public void test(){
		System.out.println(DateFormatUtils.format(1465973858191L, "yyyy-MM-dd HH:mm:ss"));
		
	}
	
	
	@Test
	public void testMsg2Json() {
		InputStream in = ParseMsg2BeanTest.class.getClassLoader().getResourceAsStream("Data");
		String msg;
		try {
			msg = IOUtils.toString(in);
			System.out.println(msg);
//			msg = StringUtils.replace(msg, "\\\"", "\"");
//			System.out.println(msg);
			JSONObject jsonObject = ParseMsg2Bean.msg2Json(msg);
			UOALConsumerDataServiceImpl uoalConsumerDataServiceImpl = new UOALConsumerDataServiceImpl();
			UserOperaTionLogBean u = uoalConsumerDataServiceImpl.transformUOTL4JSON(jsonObject);
			
			System.out.println(jsonObject);
			System.out.println(u);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeQuietly(in);
		}
		
		
		
		
	}

}

