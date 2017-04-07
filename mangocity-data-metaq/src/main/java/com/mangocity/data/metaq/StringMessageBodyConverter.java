package com.mangocity.data.metaq;

import java.io.IOException;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.metamorphosis.client.extension.spring.MessageBodyConverter;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.codec.impl.JavaDeserializer;
import com.taobao.metamorphosis.utils.codec.impl.JavaSerializer;

/**
 * 
 * 消息转化器
 * @author shilei
 * @date 2016年5月5日 下午2:55:49
 * @version
 */
public class StringMessageBodyConverter implements MessageBodyConverter<Serializable> {

	private Logger log = LoggerFactory.getLogger(StringMessageBodyConverter.class);
	
 	private JavaSerializer serializer = new JavaSerializer();
 	private JavaDeserializer deserializer = new JavaDeserializer();


    @Override
    public byte[] toByteArray(Serializable body) throws MetaClientException {
    	byte[] bs = null;
    	try {
    		if(body == null){
    			return bs;
    		}
    		if(body instanceof String){
    			bs = ((String)body).getBytes();
    		}else{
    			bs = this.serializer.encodeObject(body);
    		}
        }catch (IOException e) {
        	throw new MetaClientException(e);
        }
    	return bs;
    }
	
    @Override
	public Serializable fromByteArray(byte[] bs) throws MetaClientException {
    	synchronized (StringMessageBodyConverter.class) {
    		try {
    			String msg = new String(bs, "utf-8"); 
    			return msg;
            }
            catch (Exception e) {
            	log.error(e.getMessage(), e);
            }
    		return null;
		}
	}
	
}
