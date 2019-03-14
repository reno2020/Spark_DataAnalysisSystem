package cn.just.spark.dao.impl;

import java.util.ArrayList;
import java.util.List;

import cn.just.spark.dao.IAreaTop3ProductDAO;
import cn.just.spark.domain.AreaTop3Product;
import cn.just.spark.jdbc.JDBCHelper;

public class AreaTop3ProductDAOImpl implements IAreaTop3ProductDAO{
	/**
	 * task_id        | int(11)      | YES  |     | NULL    |       |
		| area           | varchar(255) | YES  |     | NULL    |       |
		| area_level     | varchar(255) | YES  |     | NULL    |       |
		| product_id     | int(11)      | YES  |     | NULL    |       |
		| city_infos     | varchar(255) | YES  |     | NULL    |       |
		| click_count    | int(11)      | YES  |     | NULL    |       |
		| product_name   | varchar(255) | YES  |     | NULL    |       |
		| product_status | varchar(255) | YES  |     | NULL
	 */
	
	@Override
	public void insertBatch(List<AreaTop3Product> productList) {
		List<Object[]> listParams = new ArrayList<Object[]>();
		Object[] objectParams = new Object[8];
		for(AreaTop3Product areaTop3Product :productList) {
			objectParams[0] = areaTop3Product.getTaskId();
			objectParams[1] = areaTop3Product.getArea();
			objectParams[2] = areaTop3Product.getAreaLevel();
			objectParams[3] = areaTop3Product.getProductId();
			objectParams[4] = areaTop3Product.getCityInfos();
			objectParams[5] = areaTop3Product.getClickCount();
			objectParams[6] = areaTop3Product.getProductName();
			objectParams[7] = areaTop3Product.getProductStatus();
			listParams.add(objectParams);
		}
		String sql = "INSERT INTO area_top3_product VALUES(?,?,?,?,?,?,?,?)";
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, listParams);
	}

}
