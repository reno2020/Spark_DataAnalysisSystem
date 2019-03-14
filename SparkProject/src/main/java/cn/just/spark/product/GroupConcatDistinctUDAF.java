package cn.just.spark.product;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
/**
 * 组内拼接去重函数
 * 自定义UDAF函数
 * @author shinelon
 *
 */
public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

	private static final long serialVersionUID = 1L;
	
	//指定输入数据的字段与类型
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("cityInfo", DataTypes.StringType, true)));
	//指定缓存区的字段与类型
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferCityInfo", DataTypes.StringType, true)));
	//指定返回类型
	private DataType dataType = DataTypes.StringType;
	//指定是否是确定性的
	private boolean deterministic = true;
	

	@Override
	public StructType inputSchema() {
		return inputSchema;
	}
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}
	
	/**
	 * 初始化
	 * 可认为是自己内部指定一个值
	 */
	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}
	
	/**
	 * 更新
	 * 可以认为是，一个一个地将组内的字段值传递进来
	 * 实现拼接的逻辑
	 */
	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		//缓存中的城市信息
		String bufferCityInfo = buffer.getString(0);
		//输入的城市信息
		String cityInfo = input.getString(0);
		
		//在这里要进行去重操作
		//判断之前是否拼接过该城市信息
		if(!bufferCityInfo.contains(cityInfo)) {
			if("".equals(bufferCityInfo)) {
				bufferCityInfo += cityInfo;
			}else {
				//如果之前拼接过字符串，则需要使用特定分隔符分割
				bufferCityInfo += ","+cityInfo;
			}
			buffer.update(0, bufferCityInfo);
		}
	}
	
	/**
	 * merge操作是针对一个分组的数据进行操作，有时数据分布在多个节点中
	 * 因此需要将多个节点的分组数据进行合并操作，拼接起来
	 */
	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferCityInfo1 = buffer1.getString(0);
		String bufferCityInfo2 = buffer2.getString(0);
		
		for(String cityInfo : bufferCityInfo2.split(",")) {
			if(!bufferCityInfo1.contains(cityInfo)) {
				if("".equals(bufferCityInfo1)) {
					bufferCityInfo1 += cityInfo;
				}else {
					bufferCityInfo1 += ","+cityInfo;
				}
			}
		}
		buffer1.update(0, bufferCityInfo1);
	}
	
	@Override
	public Object evaluate(Row row) {
		return row.getString(0);
	}



	

}
