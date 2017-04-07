package com.mangocity.etl.service.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import com.mangocity.etl.dao.OdsCruiseUOLMapper;
import com.mangocity.etl.service.OdsCruiseUOLService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.vo.OdsAsUOLVo;
import com.mangocity.etl.vo.OdsCruiseUOLVo;

@Service("odsCruiseUOLService")
public class OdsCruiseUOLServiceImpl implements OdsCruiseUOLService {
	
	private final static Logger LOGGER = Logger.getLogger(OdsCruiseUOLServiceImpl.class);
	
	private String tableName;
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Resource(name="odsCruiseUOLMapper")
	private OdsCruiseUOLMapper odsCruiseUOLMapper;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Override
	public int insert(OdsCruiseUOLVo odsCruiseUOLVo) {
		return odsCruiseUOLMapper.insert(odsCruiseUOLVo);
	}

	@Override
	public int batchInsert(List<OdsCruiseUOLVo> odsCruiseUOLVoList) {
		return batchInsert(odsCruiseUOLVoList,true);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int batchInsert(List<OdsCruiseUOLVo> odsCruiseUOLVoList,boolean failHandle) {
		Map[] maps = null;
		try {
			OdsCruiseUOLVo.class.getClass();
			String sql = getInsertSql();
			maps = new Map[odsCruiseUOLVoList.size()];
			int i=0;
			for (OdsCruiseUOLVo vo : odsCruiseUOLVoList) {
				Map beanMap = new BeanMap(vo);
				maps[i++] = beanMap;
			}
			int[] rets = namedParameterJdbcTemplate.batchUpdate(sql, maps);
			int count = 0; 
			for(int ret:rets){
				if(ret>0){
					count +=ret;
				}
			}
			return count;
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			LOGGER.info("errorMsg:"+errorMsg);
			if(errorMsg.contains("ORA-00001: unique constraint") && failHandle){
				Integer rowid =  null;
				OdsCruiseUOLVo odlOAsUOLVo = null;
				List<OdsCruiseUOLVo> voList = new ArrayList<OdsCruiseUOLVo>();
				for(OdsCruiseUOLVo odsAsUOLVo:odsCruiseUOLVoList){
					rowid = odsAsUOLVo.getSourceRowId();
					odlOAsUOLVo = odsCruiseUOLMapper.findById(rowid);
					if(odlOAsUOLVo==null){//数据库里不存在
						voList.add(odsAsUOLVo);
					}
				}
				if(CollectionUtils.isNotEmpty(voList)){
					batchInsert(voList,false);
				}
			}else{
				LOGGER.error("insert fail:",e);
				StringBuilder buff = new StringBuilder("[");
				if(maps!=null){
					for (Map beanMap: maps) {
						buff.append(beanMap.get(Constants.SOURCE_ROW_ID_FIELD_NAME)).append(",");
					}
				}
				buff.deleteCharAt(buff.length()-1);
				buff.append("]");
				LOGGER.error("insert fail sourceRowIds:"+buff.toString());
			}
			
		}
		return -1;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private String getInsertSql() {
		OdsCruiseUOLVo vo = new OdsCruiseUOLVo();
		Map tmpMap = new BeanMap(vo);
		Map beanMap = new LinkedHashMap();
		beanMap.putAll(tmpMap);
		beanMap.remove("class");
		List<String> columnList = new ArrayList<String>(beanMap.keySet());
		List<String> valueNameList = new ArrayList<String>();
		for (String column : columnList) {
			valueNameList.add(":"+column);
		}
		String columns = StringUtils.join(columnList, ",");
		String valueNames = StringUtils.join(valueNameList, ",");
		String sql = "insert into "+ tableName +"("+columns+") values("+valueNames+")";
		return sql;
	}
	

	@Override
	public int findMaxSourceRowId() {
		String sourceRowId = "sourcerowid";
		return odsCruiseUOLMapper.findMaxFieldValue(sourceRowId);
	}
	
	public static void main(String[] args) {
		System.out.println(new OdsCruiseUOLServiceImpl().getInsertSql());
	}
	
}
