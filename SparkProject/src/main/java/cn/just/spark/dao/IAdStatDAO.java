package cn.just.spark.dao;

import java.util.List;

import cn.just.spark.domain.AdStat;
/**
 * 广告流量实时统计
 * 2018-11-25
 * @author shinelon
 *
 */
public interface IAdStatDAO {
	
	void updateBatch(List<AdStat> adStat);

}
