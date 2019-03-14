package cn.just.spark.dao;

import java.util.List;

import cn.just.spark.domain.AdBlockList;

/**
 * 用户黑名单DAO接口
 * @author shinelon
 *
 */
public interface IAdBlockListDAO {
	
	public void insertBatch(List<AdBlockList> blockList);
	
	public List<AdBlockList> findAll();

}
