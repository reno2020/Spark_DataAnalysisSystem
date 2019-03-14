package cn.just.spark.dao;

import java.util.List;

import cn.just.spark.domain.AreaTop3Product;

public interface IAreaTop3ProductDAO {
	void insertBatch(List<AreaTop3Product> productList); 
}
