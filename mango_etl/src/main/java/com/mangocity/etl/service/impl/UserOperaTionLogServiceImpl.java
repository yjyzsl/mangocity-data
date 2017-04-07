package com.mangocity.etl.service.impl;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.mangocity.etl.dao.UserOperaTionLogMapper;
import com.mangocity.etl.service.UserOperaTionLogService;
import com.mangocity.etl.vo.OdsCruiseUOLVo;
import com.mangocity.etl.vo.UserOperaTionLogVo;

@Service("userOperaTionLogService")
public class UserOperaTionLogServiceImpl implements UserOperaTionLogService {
	
	public final static Integer ETL_LOAD_TAG = 100;

	@Resource(name="userOperaTionLogMapper")
	private UserOperaTionLogMapper userOperaTionLogMapper; 
	
	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	@Override
	public List<UserOperaTionLogVo> findAll() {
		return userOperaTionLogMapper.findAll();
	}

	@Override
	public UserOperaTionLogVo findById(Integer sourcerowid) {
		return userOperaTionLogMapper.findById(sourcerowid);
	}

	@Override
	public List<UserOperaTionLogVo> findByChannel(String channel) {
		return userOperaTionLogMapper.findByChannel(channel);
	}
	
	@Override
	public List<UserOperaTionLogVo> find(UserOperaTionLogVo userOperaTionLogVo) {
		return userOperaTionLogMapper.find(userOperaTionLogVo);
	}
	
	@Override
	public Integer findCount(UserOperaTionLogVo userOperaTionLogVo) {
		return userOperaTionLogMapper.findCount(userOperaTionLogVo);
	}

	@Override
	public Integer findMaxSourceRowIdByChannel(UserOperaTionLogVo userOperaTionLogVo) {
		return userOperaTionLogMapper.findMaxSourceRowIdByChannel(userOperaTionLogVo);
	}

	@Override
	public List<UserOperaTionLogVo> findPageBySourcerowid(Map params) {
		return userOperaTionLogMapper.findPageBySourcerowid(params);
	}
	

	@Override
	public List<UserOperaTionLogVo> findPageByRowNum(Map params) {
		return userOperaTionLogMapper.findPageByRowNum(params);
	}

	@Override
	public Integer findMinSourceRowIdByChannel(UserOperaTionLogVo userOperaTionLogVo) {
		return userOperaTionLogMapper.findMinSourceRowIdByChannel(userOperaTionLogVo);
	}

	@Override
	public Integer findMaxSourceRowId() {
		return this.userOperaTionLogMapper.findMaxSourceRowId();
	}

	
	
}
