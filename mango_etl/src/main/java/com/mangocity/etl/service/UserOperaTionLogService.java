package com.mangocity.etl.service;

import java.util.List;
import java.util.Map;

import com.mangocity.etl.vo.UserOperaTionLogVo;

public interface UserOperaTionLogService {
	
	/**
	 * 查询所有
	 * @return
	 */
	List<UserOperaTionLogVo> findAll();
	
	/**
	 * 查询所有
	 * @return
	 */
	List<UserOperaTionLogVo> find(UserOperaTionLogVo userOperaTionLogVo);
	
	/**
	 * 按条件统计数量
	 * @param userOperaTionLogVo
	 * @return
	 */
	Integer findCount(UserOperaTionLogVo userOperaTionLogVo);
	
	/**
	 * 根据ID查询
	 * @param sourcerowid
	 * @return
	 */
	UserOperaTionLogVo findById(Integer sourcerowid);
	
	/**
	 * 根据渠道查询
	 * @param channel 来源渠道 值为当前页面域名
	 * @return
	 */
	List<UserOperaTionLogVo> findByChannel(String channel);
	
	/**
	 * 根据Sourcerowid进行分页查询
	 * @param params
	 * @return
	 */
	List<UserOperaTionLogVo> findPageBySourcerowid(Map params);
	
	/**
	 * 根据rownum进行分页查询
	 * @param params
	 * @return
	 */
	List<UserOperaTionLogVo> findPageByRowNum(Map params);
	
	/**
	 * 根据通道查最大的sourcerowid
	 * @param channel
	 * @return
	 */
	Integer findMaxSourceRowIdByChannel(UserOperaTionLogVo userOperaTionLogVo);

	/**
	 * 查最大的sourcerowid
	 * @return
	 */
	Integer findMaxSourceRowId();
	/**
	 * 根据通道查最小的sourcerowid
	 * @param channel
	 * @return
	 */
	Integer findMinSourceRowIdByChannel(UserOperaTionLogVo userOperaTionLogVo);
}
