package cn.just.spark.dao.impl;

import cn.just.spark.dao.ITop10CategorySessionDAO;
import cn.just.spark.domain.Top10CategorySession;
import cn.just.spark.jdbc.JDBCHelper;
/**
 * DAO接口实现类
 * 2018-08-07
 * @author shinelon
 *
 */
public class Top10CategorySessionDAOImpl implements ITop10CategorySessionDAO{

	@Override
	public void insert(Top10CategorySession top10CategorySession) {
		String sql = "insert into top10_category_session values(?,?,?,?)";
		Object[] params=new Object[] {
			top10CategorySession.getTaskId(),
			top10CategorySession.getCategoryId(),
			top10CategorySession.getSessionId(),
			top10CategorySession.getClickCount()
		};
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}


}
