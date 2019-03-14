package cn.just.spark.dao;

import cn.just.spark.domain.Task;

/**
 * DAO层task接口
 * 2018-7-31
 * @author shinelon
 *
 */
public interface ITaskDAO {
	public Task findById(long taskId);
}
