package cn.just.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import com.alibaba.fastjson.JSONObject;


/**
 * 自定义UDF函数
 * 从一个json串中获取某一个字段的值
 * @author shinelon
 *
 */
public class GetJsonObjectUDF implements UDF2<String, String, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String json, String field) throws Exception {
		try {
			JSONObject jsonObject = JSONObject.parseObject(json);
			return jsonObject.getString(field);
			
		}catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
