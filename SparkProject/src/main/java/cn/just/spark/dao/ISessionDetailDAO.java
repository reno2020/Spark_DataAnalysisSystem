package cn.just.spark.dao;

import java.util.List;

import cn.just.spark.domain.SessionDetail;

/**
 * session明细数据DAO接口
 * 2018-08-05
 * @author shinelon
 *
 */
public interface ISessionDetailDAO {
	public void insert(SessionDetail sessionDetail);
	public void executeBatch(List<SessionDetail> list);
}
