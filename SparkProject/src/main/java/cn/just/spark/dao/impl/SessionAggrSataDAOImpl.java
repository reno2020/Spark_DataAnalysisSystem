package cn.just.spark.dao.impl;

import cn.just.spark.dao.ISessionAggrSataDAO;
import cn.just.spark.domain.SessionAggrSata;
import cn.just.spark.jdbc.JDBCHelper;

public class SessionAggrSataDAOImpl implements ISessionAggrSataDAO{
	
	/**
	 * 插入数据
	 */
	@Override
	public void insert(SessionAggrSata sessionAggrSata) {
		String sql="insert into session_aggr_stat values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params=new Object[] {
			sessionAggrSata.getTaskId(),
			sessionAggrSata.getSessionCount(),
			sessionAggrSata.getVisit_length_1s_3s_ratio(),
			sessionAggrSata.getVisit_length_4s_6s_ratio(),
			sessionAggrSata.getVisit_length_7s_9s_ratio(),
			sessionAggrSata.getVisit_length_10s_30s_ratio(),
			sessionAggrSata.getVisit_length_30s_60s_ratio(),
			sessionAggrSata.getVisit_length_1m_3m_ratio(),
			sessionAggrSata.getVisit_length_3m_10m_ratio(),
			sessionAggrSata.getVisit_length_10m_30m_ratio(),
			sessionAggrSata.getVisit_length_30m_ratio(),
			sessionAggrSata.getStep_length_1_3_ratio(),
			sessionAggrSata.getStep_length_4_6_ratio(),
			sessionAggrSata.getStep_length_7_9_ratio(),
			sessionAggrSata.getStep_length_10_30_ratio(),
			sessionAggrSata.getStep_length_30_60_ratio(),
			sessionAggrSata.getStep_length_60_ratio()
		};
		//获取JDBC组件单例
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
