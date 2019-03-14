package cn.just.spark.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import cn.just.spark.dao.IAdStatDAO;
import cn.just.spark.domain.AdStat;
import cn.just.spark.jdbc.JDBCHelper;
import cn.just.spark.model.AdStatQueryResult;

public class AdStatDAOImpl implements IAdStatDAO{

	@Override
	public void updateBatch(List<AdStat> adStat) {
		List<AdStat> insertList = new ArrayList<AdStat>();
		List<AdStat> updateList = new ArrayList<AdStat>();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String selectSQL = "SELECT COUNT(*) FROM ad_stat "
							+ "WHERE date=? AND "
							+ "province=? AND "
							+ "city=? AND "
							+ "ad_id=?";
		for(AdStat as:adStat) {
			String date = as.getDate();
			String province = as.getProvince();
			String city = as.getCity();
			Long adId = as.getAdId();
			Object[] params = {
				date,province,city,adId
			};
			
			final AdStatQueryResult adStatQueryResult = new AdStatQueryResult();
			jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallBack() {
				
				@Override
				public void processor(ResultSet rs) throws Exception {
					if(rs.next()) {
						Long clickCount = Long.valueOf(String.valueOf(rs.getInt(1)));
						adStatQueryResult.setCount(clickCount);
					}
				}
			});
			Long clickCount = adStatQueryResult.getCount();
			//如果数据库中有这条key，则加入更新队列中，否则加入到插入队列中
			if(clickCount>0) {
				updateList.add(as);
			}else {
				insertList.add(as);
			}
		}
		
		String insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";
		List<Object[]> insertParams = new ArrayList<Object[]>();
		for(AdStat as:insertList) {
			Object[] params = {
					as.getDate(),
					as.getProvince(),
					as.getCity(),
					as.getAdId(),
					as.getClickCount()
			};
			insertParams.add(params);
		}
		jdbcHelper.executeBatch(insertSQL, insertParams);
		
		String updateSQL = "UPDATE ad_stat SET click_count=? "
				+ "WHERE date=? AND "
				+ "province=? AND "
				+ "city=? AND "
				+ "ad_id=?";
		List<Object[]> updateParams = new ArrayList<Object[]>();
		for(AdStat as:updateList) {
			Object[] params = {
					as.getClickCount(),
					as.getDate(),
					as.getProvince(),
					as.getCity(),
					as.getAdId()
			};
			updateParams.add(params);
		}
		jdbcHelper.executeBatch(updateSQL, updateParams);
	}

}
