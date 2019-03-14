package cn.just.spark.dao.impl;

import cn.just.spark.dao.ITop10CategoryDAO;
import cn.just.spark.domain.Top10Category;
import cn.just.spark.jdbc.JDBCHelper;
/**
 * top10热门品类DAO实现类
 * @author shinelon
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO{

	@Override
	public void insert(Top10Category top10Category) {

		String sql="insert into top10_category values(?,?,?,?,?)";
		Object[] params=new Object[] {
			top10Category.getTaskId(),
			top10Category.getCategoryId(),
			top10Category.getClickCount(),
			top10Category.getOrderCount(),
			top10Category.getPayCount()
		};
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
			
	}

}
