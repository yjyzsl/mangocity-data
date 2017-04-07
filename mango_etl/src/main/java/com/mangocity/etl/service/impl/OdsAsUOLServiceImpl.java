package com.mangocity.etl.service.impl;

import java.sql.SQLException;
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

import com.mangocity.etl.dao.OdsAsUOLMapper;
import com.mangocity.etl.service.OdsAsUOLService;
import com.mangocity.etl.util.Constants;
import com.mangocity.etl.vo.OdsAsUOLVo;

@Service("odsAsUOLService")
public class OdsAsUOLServiceImpl implements OdsAsUOLService {
	
	private final static Logger LOGGER = Logger.getLogger(OdsAsUOLServiceImpl.class);
	
	private String tableName;
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Resource(name="odsAsUOLMapper")
	private OdsAsUOLMapper odsAsUOLMapper;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Override
	public int insert(OdsAsUOLVo odsAsUOLVo) {
		return odsAsUOLMapper.insert(odsAsUOLVo);
	}

	@Override
	public int batchInsert(List<OdsAsUOLVo> odsAsUOLVoList) {
		return batchInsert(odsAsUOLVoList,true);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int batchInsert(List<OdsAsUOLVo> odsAsUOLVoList,boolean failHandle) {
		Map[] maps = null;
		try {
			OdsAsUOLVo.class.getClass();
			String sql = getInsertSql();
			maps = new Map[odsAsUOLVoList.size()];
			int i=0;
			for (OdsAsUOLVo vo : odsAsUOLVoList) {
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
		}catch (Exception e) {
			String errorMsg = e.getMessage();
			LOGGER.info("errorMsg:"+errorMsg);
			if(errorMsg.contains("ORA-00001: unique constraint") && failHandle){
				Integer rowid =  null;
				OdsAsUOLVo odlOAsUOLVo = null;
				List<OdsAsUOLVo> voList = new ArrayList<OdsAsUOLVo>();
				for(OdsAsUOLVo odsAsUOLVo:odsAsUOLVoList){
					rowid = odsAsUOLVo.getSourceRowId();
					odlOAsUOLVo = odsAsUOLMapper.findById(rowid);
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
		OdsAsUOLVo vo = new OdsAsUOLVo();
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
		return odsAsUOLMapper.findMaxFieldValue(sourceRowId);
	}
	
	
}
