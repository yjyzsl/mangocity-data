package com.mangocity.data.commons.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 *
 * @author shilei
 * @date 2016年5月5日 下午5:41:19 
 */
public class GenericUtil {

	/**
	 * 获取类上泛型的class
	 *
	 * @author shilei
	 * @date 2016年5月5日 下午5:45:16
	 * @param clazz
	 * @param index
	 * @return
	 */
	public static Class<?> getGenericType(Class<?> clazz,int index){  
		Class<?> genericClazz = null;
		if(clazz==null){
			return genericClazz;
		}
        Type[] t = ((ParameterizedType)clazz.getGenericSuperclass()).getActualTypeArguments();  
        if(t != null && t.length>=index){
        	genericClazz = (Class<?>)t[index];
        }
        return genericClazz;
    }  
	
}

