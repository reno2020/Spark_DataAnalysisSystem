package cn.just.spark.dao;

import java.util.List;

import cn.just.spark.domain.AdUserClickCount;

public interface IAdUserClickCountDAO {
	
	//批量更新每天指定用户对指定广告的点击次数
	public void updateBatch(List<AdUserClickCount> adUserClickCountList);
	
	//使用多条件查询用户广告点击次数
	public Long findClickCountByMultiKey(String date,Long userId,Long adId);
	

}
