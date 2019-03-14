package cn.just.spark.dao.impl;

import cn.just.spark.dao.ISessionRandomExtractDAO;
import cn.just.spark.domain.SessionRandomExtract;
import cn.just.spark.jdbc.JDBCHelper;

/**
 * 随机抽取DAO实现类
 * @author shinelon
 *
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO{

	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params=new Object[] {
				sessionRandomExtract.getTaskId(),
				sessionRandomExtract.getSessionId(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()
		};
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
