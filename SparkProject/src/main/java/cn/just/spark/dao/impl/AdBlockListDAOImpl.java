package cn.just.spark.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.just.spark.dao.IAdBlockListDAO;
import cn.just.spark.domain.AdBlockList;
import cn.just.spark.jdbc.JDBCHelper;
/**
 * 用户动态黑名单生成DAO
 * @author shinelon
 *
 */
public class AdBlockListDAOImpl implements IAdBlockListDAO{

	@Override
	public void insertBatch(List<AdBlockList> blockList) {
		String insertSQL = "INSERT INTO ad_blacklist values(?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(AdBlockList adBlockList:blockList) {
			Long userId = adBlockList.getUserId();
			Object[] params = {
					userId
			};
			paramsList.add(params);
		}
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(insertSQL, paramsList);
	}

	/**
	 * 查询所有黑名单用户
	 */
	@Override
	public List<AdBlockList> findAll() {
		String sql = "SELECT * FROM ad_blacklist";
		final List<AdBlockList> blockList = new ArrayList<AdBlockList>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallBack() {
			
			@Override
			public void processor(ResultSet rs) throws Exception {
				Long userId = Long.valueOf(String.valueOf(rs.getInt(1)));
				AdBlockList adBlockList = new AdBlockList();
				adBlockList.setUserId(userId);
				blockList.add(adBlockList);
			}
		});
		return blockList;
	}

}
