package cn.just.spark.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
/**
 * fastJson测试类
 * 2018-7-31
 * @author shinelon
 *
 */
public class FastJsonTest {
	
	public static void main(String[] args) {
		String json="[{'name':'shinelon','age':'20'}]";
		JSONArray jsonArray=JSONArray.parseArray(json);
		JSONObject jsonObject=jsonArray.getJSONObject(0);
		System.out.println(jsonObject.getString("name"));
	}
	

}
