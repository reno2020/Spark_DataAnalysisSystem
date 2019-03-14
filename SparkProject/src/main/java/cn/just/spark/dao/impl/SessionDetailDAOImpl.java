package cn.just.spark.dao.impl;

import java.util.ArrayList;
import java.util.List;

import cn.just.spark.dao.ISessionDetailDAO;
import cn.just.spark.domain.SessionDetail;
import cn.just.spark.jdbc.JDBCHelper;
/**
 * 2018-08-05
 * @author shinelon
 *
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO{

	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql="insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params=new Object[] {
			sessionDetail.getTaskId(),
			sessionDetail.getUserId(),
			sessionDetail.getSessionId(),
			sessionDetail.getPageId(),
			sessionDetail.getActionTime(),
			sessionDetail.getSearchKeyword(),
			sessionDetail.getClickCategoryId(),
			sessionDetail.getClickProductId(),
			sessionDetail.getOrderCategoryIds(),
			sessionDetail.getOrderProductIds(),
			sessionDetail.getPayCategoryIds(),
			sessionDetail.getPayProductIds()
		};
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	/**
	 * 批量插入数据
	 */
	@Override
	public void executeBatch(List<SessionDetail> list) {
		String sql="insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		List<Object[]> paramsList=new ArrayList<Object[]>();
		for(SessionDetail sessionDetail:list) {
			Object[] params=new Object[] {
					sessionDetail.getTaskId(),
					sessionDetail.getUserId(),
					sessionDetail.getSessionId(),
					sessionDetail.getPageId(),
					sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()
				};
			paramsList.add(params);
		}
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}

}
