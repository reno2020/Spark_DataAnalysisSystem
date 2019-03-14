package cn.just.spark.dao.impl;

import cn.just.spark.dao.IPageSplitConvertRateDAO;
import cn.just.spark.domain.PageSplitConvertRate;
import cn.just.spark.jdbc.JDBCHelper;
/**
 * 页面切片转换率DAO实现类
 * @author shinelon
 *
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO{

	@Override
	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";
		Object[] objectParams = new Object[] {
			pageSplitConvertRate.getTaskId(),
			pageSplitConvertRate.getConvertRate()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, objectParams);
	}
}
