package com.mangocity.etl.dao;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.mangocity.etl.vo.OdsAsUOLVo;

@Repository("odsAsUOLMapper")
public interface OdsAsUOLMapper {

	/**
	 * 新增
	 * @param odsCruiseUOLVo
	 * @return
	 */
	int insert(OdsAsUOLVo odsAsUOLVo);
	
	/**
	 * 批量新增
	 * @param odsCruiseUOLVoList
	 * @return
	 */
	int batchInsert(List<OdsAsUOLVo> odsAsUOLVoList);
	
	/**
	 * 查询最大的SourceRowId
	 * @return
	 */
	int findMaxFieldValue(String fieldName);
	
	OdsAsUOLVo findById(Integer id); 
}
