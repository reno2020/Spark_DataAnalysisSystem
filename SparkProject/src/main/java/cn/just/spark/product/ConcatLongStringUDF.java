package cn.just.spark.product;

import org.apache.spark.sql.api.java.UDF3;
/**
 * 实现字符串的拼接（使用指定分隔符）
 * 自定义UDF函数
 * @author shinelon
 *
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String call(Long t1, String t2, String split) throws Exception {
		return String.valueOf(t1) + split + t2;
	}

}
