package com.mangocity.etl.service;

import java.util.List;

import com.mangocity.etl.vo.OdsCruiseUOLVo;

public interface OdsCruiseUOLService {

	/**
	 * 设置表面
	 *
	 * @author shilei
	 * @date 2016年4月22日 下午3:05:25
	 * @param tableName
	 */
	public void setTableName(String tableName);
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
	int findMaxSourceRowId();
}
