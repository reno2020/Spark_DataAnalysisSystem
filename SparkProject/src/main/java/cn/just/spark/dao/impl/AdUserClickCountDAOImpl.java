package cn.just.spark.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.just.spark.dao.IAdUserClickCountDAO;
import cn.just.spark.domain.AdUserClickCount;
import cn.just.spark.jdbc.JDBCHelper;
import cn.just.spark.jdbc.JDBCHelper.QueryCallBack;
import cn.just.spark.model.AdUserClickCountResult;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO{

	@Override
	public void updateBatch(List<AdUserClickCount> adUserClickCountList) {
		//检查数据库，对数据的操作进行分类
		List<AdUserClickCount> insertAdUserClickCountList = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCountList = new ArrayList<AdUserClickCount>();
		
		String selectSQL = "select count(*) from ad_user_click_count "
				+ "where date=? and user_id=? and ad_id=?";
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		final AdUserClickCountResult adUserClickCountResult = new AdUserClickCountResult();
		
		for(AdUserClickCount adUserClickCount:adUserClickCountList) {
			Object[] objectParams = {
					adUserClickCount.getDate(),
					adUserClickCount.getUserId(),
					adUserClickCount.getAdId()
			};
			jdbcHelper.executeQuery(selectSQL, objectParams, new QueryCallBack() {
				@Override
				public void processor(ResultSet rs) throws Exception {
					if(rs.next()) {
						int count = rs.getInt(1);
						adUserClickCountResult.setCount(Long.valueOf(count));
					}
				}
			});
			
			//如果数据库中存在该数据
			if(adUserClickCountResult.getCount()>0) {
				updateAdUserClickCountList.add(adUserClickCount);
			}else {
				insertAdUserClickCountList.add(adUserClickCount);
			}
		}
		
		//批量更新操作
		String updateSQL = "update ad_user_click_count set click_count=click_count+? where date=? and user_id=? and ad_id=?";
		List<Object[]> updateParams = new ArrayList<Object[]>();
		
		for(AdUserClickCount adUserClickCount:updateAdUserClickCountList) {
			Object[] params = {
				adUserClickCount.getClickCount(),
				adUserClickCount.getDate(),
				adUserClickCount.getUserId(),
				adUserClickCount.getAdId()
			};
			updateParams.add(params);
		}
		jdbcHelper.executeBatch(updateSQL, updateParams);
		
		//批量插入操作
		String insertSQL = "insert into ad_user_click_count values(?,?,?,?)";
		List<Object[]> insertParams = new ArrayList<Object[]>();
		
		for(AdUserClickCount adUserClickCount:insertAdUserClickCountList) {
			Object[] params = {
					adUserClickCount.getDate(),
					adUserClickCount.getUserId(),
					adUserClickCount.getAdId(),
					adUserClickCount.getClickCount()
			};
			insertParams.add(params);
		}
		jdbcHelper.executeBatch(insertSQL, insertParams);
	}
	
	/**
	 * 多条件查询用户广告点击次数
	 */
	@Override
	public Long findClickCountByMultiKey(String date, Long userId, Long adId) {
		String selectSQL ="SELECT click_count FROM ad_user_click_count"
				+ " WHERE date=? AND user_id=? AND ad_id=?";
		Object[] params = {
				date,userId,adId
		};
		
		final AdUserClickCountResult adUserClickCountResult = new AdUserClickCountResult();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeQuery(selectSQL, params, new QueryCallBack() {
			
			@Override
			public void processor(ResultSet rs) throws Exception {
				int clickCount = rs.getInt(1);
				adUserClickCountResult.setClickCount(Long.valueOf(clickCount));
			}
		});
		Long clickCount = adUserClickCountResult.getClickCount();
		return clickCount;
	}

}
