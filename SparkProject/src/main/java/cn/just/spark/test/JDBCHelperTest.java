package cn.just.spark.test;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import cn.just.spark.jdbc.JDBCHelper;
import cn.just.spark.jdbc.JDBCHelper.QueryCallBack;

/**
 * JDBC辅助组件测试类
 * @author shinelon
 *
 */
public class JDBCHelperTest {
	public static void main(String[] args) {
		JDBCHelper test=JDBCHelper.getInstance();
		/**
		 * 测试插入数据
		 */
//		test.executeUpdate(
//				"insert into test(name,age) values(?,?)",
//				new Object[]{"张三",20});
		/**
		 * 测试查询结果
		 */
		final Map<String ,Object> map=new HashMap<String,Object>();
		test.executeQuery(
				"select * from test where name=?", 
				new Object[] {"张三"}, 
				new QueryCallBack() {
					@Override
					public void processor(ResultSet rs) throws Exception {
						if(rs.next()) {
							String name=rs.getString(1);
							int age=rs.getInt(2);
							map.put("name", name);
							map.put("age", age);
						}
					}
				});
		System.out.println(map.get("name")+" : "+map.get("age"));
	}

}
