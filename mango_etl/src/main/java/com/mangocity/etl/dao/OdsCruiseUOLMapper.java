package com.mangocity.etl.dao;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.mangocity.etl.vo.OdsCruiseUOLVo;

@Repository("odsCruiseUOLMapper")
public interface OdsCruiseUOLMapper {

	/**
	 * 新增
	 * @param odsCruiseUOLVo
	 * @return
	 */
	int insert(OdsCruiseUOLVo odsCruiseUOLVo);
	
	/**
	 * 批量新增
	 * @param odsCruiseUOLVoList
	 * @return
	 */
	int batchInsert(List<OdsCruiseUOLVo> odsCruiseUOLVoList);
	
	/**
	 * 查询最大的SourceRowId
	 * @return
	 */
	int findMaxFieldValue(String fieldName);
	
	OdsCruiseUOLVo findById(Integer id); 
}
