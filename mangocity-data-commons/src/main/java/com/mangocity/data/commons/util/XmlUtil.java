package com.mangocity.data.commons.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

/**
 *
 * @author shilei
 * @date 2016年5月6日 上午11:01:12 
 */
public class XmlUtil {
	
	/**
	 * 解析xml中根元素下的子元素
	 * 子元素的元素名作为map key内容能作为value
	 *
	 * @author shilei
	 * @date 2016年5月6日 上午11:06:25
	 * @param xmlIn
	 * @return
	 * @throws DocumentException
	 */
	public static List<Map<String,String>> parseXml(InputStream xmlIn) throws DocumentException {
		List<Map<String,String>> result = null;
		try {
			SAXReader reader = new SAXReader();
			Document document = reader.read(xmlIn);
			Element node = document.getRootElement();
			List<Element> eleList = getElements(node);
			
			result = new ArrayList<Map<String,String>>(eleList.size());
			String elementName = null;
			List<Element> subEleLsit = null;
			Map<String,String> eleMap = null;
			for (Element ele : eleList) {
				subEleLsit = getElements(ele);
				eleMap = new HashMap<String,String>(subEleLsit.size());
				for (Element subEle : subEleLsit) {
					elementName = subEle.getName();
					eleMap.put(elementName, (String)subEle.getData());
				}
				result.add(eleMap);
			}
		} catch (DocumentException e) {
			throw e;
		} 
		return result;
	}
	
	
	/**
	 * 遍历出元素下的子元素
	 *
	 * @author shilei
	 * @date 2016年5月6日 上午11:03:45
	 * @param element
	 * @return
	 */
	public static List<Element> getElements(Element element) {
		List<Element> eleList = null;
		if (element != null) {
			eleList = new ArrayList<Element>();
			@SuppressWarnings("unchecked")
			Iterator<Element> it = element.elementIterator();
			while (it.hasNext()) {
				Element e = it.next();
				eleList.add(e);
			}
		}
		return eleList;
	}

}

