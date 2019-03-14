package cn.just.spark.product;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.alibaba.fastjson.JSONObject;

import cn.just.spark.conf.ConfigurationManager;
import cn.just.spark.constant.Constants;
import cn.just.spark.dao.IAreaTop3ProductDAO;
import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.dao.impl.DAOFactory;
import cn.just.spark.domain.AreaTop3Product;
import cn.just.spark.domain.Task;
import cn.just.spark.utils.ParamUtils;
import cn.just.spark.utils.SparkUtils;
import scala.Tuple2;

/**
 * @since 2018-10-30
 * 各个区域内top3热门商品spark作业
 * @author shinelon
 *
 */
public class AreaTop3productSpark {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("AreaTop3productSpark");
		
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
//		sqlContext.setConf("spark.sql.shuffle.partitions", "1000");
//		sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold","20971520");
		
		/**
		 * 注册自定义函数
		 */
		//字符串拼接函数 (函数名，自定义函数，返回类型)
		sqlContext.udf().register("concat_long_string", 
				new ConcatLongStringUDF(), DataTypes.StringType);
		
		sqlContext.udf().register("get_json_object", 
				new GetJsonObjectUDF(), DataTypes.StringType);
		
		//组内拼接去重函数
		sqlContext.udf().register("group_concat_distinct", 
				new GroupConcatDistinctUDAF());
		
		SparkUtils.mockData(sc, sqlContext);
		
		ITaskDAO taskDAO=DAOFactory.getTaskDAO();
		
		long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
		
		Task task = taskDAO.findById(taskId);
		
		//{"startDate":["2018-10-30"],"endDate":["2018-10-30"]}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		String startDate = ParamUtils.getParam(taskParam, Constants.TASK_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.TASK_END_DATE);
		
		//获取指定范围内的用户点击行为数据
		JavaPairRDD<Long,Row> cityId2ActionPairRDD = getCityId2ClickActionRDDByDate(sqlContext,startDate,endDate);
		
		//获取城市信息
		JavaPairRDD<Long,Row> cityId2CityInfoPairRDD = getCityId2CityInfoPairRDD(sqlContext);
		
		//点击行为数据和城市信息join操作并且注册为临时表
		joinAndGenerateTempTable(sqlContext,cityId2ActionPairRDD,cityId2CityInfoPairRDD);
		
		//生成各区域各个商品点击次数城市列表信息临时表
		generAreaProdClickCountCityInfoTempTable(sqlContext);
		
		//生成各区域各个商品点击次数以及商品信息临时表
		generAreaFullProdClickCountTempTable(sqlContext);
		
		//获取各区域top3热门商品信息
		JavaRDD<Row> areaTop3Product = getAreaTop3ProductRDD(sqlContext);
		
		List<Row> productList = areaTop3Product.collect();
		
		//持久化各区域热门商品信息到数据库
		persistAreaTop3Product(taskId,productList);
		
	}
	
	/**
	 * 查询指定范围内的点击行为数据
	 * @param sqlContext
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private static JavaPairRDD<Long,Row> getCityId2ClickActionRDDByDate(SQLContext sqlContext,String startDate,String endDate){
		// 从user_visit_action中，查询用户访问行为数据
				// 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
				// 第二个限定：在用户指定的日期范围内的数据
				
				String sql = 
						"SELECT "
							+ "city_id,"
							+ "click_product_id product_id "
							+ "FROM user_visit_action "
							+ "WHERE click_product_id IS NOT NULL "			
							+ "AND date>='" + startDate + "' "
							+ "AND date<='" + endDate + "'";
				
				DataFrame clickActionDF = sqlContext.sql(sql);
				
				JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();
				
				return clickActionRDD.mapToPair(new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						Long cityId = row.getLong(0);
						return new Tuple2<Long,Row>(cityId,row);
					}
				});
	}
	
	/**
	 * 获取城市信息
	 * @param sqlContext
	 * @return
	 */
	public static JavaPairRDD<Long,Row> getCityId2CityInfoPairRDD(SQLContext sqlContext){
		String url=null;
		String username=null;
		String password=null;
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			url = ConfigurationManager.getProperties(Constants.JDBC_URL);
			username = ConfigurationManager.getProperties(Constants.JDBC_USER);
			password = ConfigurationManager.getProperties(Constants.JDBC_PASSWORD);
		}else {
			url = ConfigurationManager.getProperties(Constants.JDBC_URL_PROD);
			username = ConfigurationManager.getProperties(Constants.JDBC_USER_PROD);
			password = ConfigurationManager.getProperties(Constants.JDBC_PASSWORD_PROD);
		}
		Map<String,String> options = new HashMap<String,String>();
		options.put("url", url);
		options.put("dbtable", "city_info");
		options.put("user", username);
		options.put("password", password);
		
		DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();
		
		JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
		
		return cityInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				Long cityInfo = row.getLong(0);
				return new Tuple2<Long,Row>(cityInfo,row);
			}
		});
	}
	/**
	 * 点击行为数据和用户城市信息进行join操作，
	 * 并且将其转换为DataFrame注册为临时表
	 * @param sqlContext
	 * @param cityId2ActionPairRDD
	 * @param cityId2CityInfoPairRDD
	 */
	private static void joinAndGenerateTempTable(
			SQLContext sqlContext,
			JavaPairRDD<Long,Row> cityId2ActionPairRDD,
			JavaPairRDD<Long,Row> cityId2CityInfoPairRDD) {
		//进行join操作
		JavaPairRDD<Long,Tuple2<Row,Row>> joinClickActionAndCityInfo = cityId2ActionPairRDD.join(cityId2CityInfoPairRDD);

		JavaRDD<Row> clickActionAndCityInfoMapRDD = joinClickActionAndCityInfo.map(
				
				new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
				Long cityId = tuple._1;
				Row actionClick = tuple._2._1;
				Row cityInfo = tuple._2._2;
				Long productId = actionClick.getLong(1);
				String cityName = cityInfo.getString(1);
				String area = cityInfo.getString(2);
				return RowFactory.create(cityId,cityName,area,productId);
			}
		});
		
		//定义临时表字段信息
		List<StructField> fieldList = new ArrayList<StructField>();
		fieldList.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
		fieldList.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
		fieldList.add(DataTypes.createStructField("area", DataTypes.StringType, true));
		fieldList.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
		
		StructType schema = DataTypes.createStructType(fieldList);
		
		DataFrame df = sqlContext.createDataFrame(clickActionAndCityInfoMapRDD, schema);
		//注册为临时表
		df.registerTempTable("tmp_click_product_basic");
	}
	
	/**
	 * 生成各区域各个商品点击次数城市列表信息
	 * @param sqlContext
	 */
	private static void generAreaProdClickCountCityInfoTempTable(SQLContext sqlContext) {
		String sql = "SELECT "
						+ "area,"
						+ "product_id,"
						+ "count(*) click_count,"
						+ "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
					+ "FROM tmp_click_product_basic "
					+ "GROUP BY "
					+ "area,product_id";
		
		DataFrame df = sqlContext.sql(sql);
		
		//将查询的信息注册为一个临时表
		//各区域各个商品的点击次数以及城市列表信息
		df.registerTempTable("tmp_area_product_click_count");
	}
	
	/**
	 * 生成各区域各商品点击次数信息以及商品详细信息临时表
	 * @param sqlContext
	 */
	private static void generAreaFullProdClickCountTempTable(SQLContext sqlContext) {
		//将之前各个区域商品点击表的信息用Product_id去
		//关联商品信息表，product_id,product_name,product_status
		// product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
		//使用自定义UDF函数get_json_object函数从json串中获取字段信息值，然后使用内置If函数判断字段
		//类型，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
		String sql = "SELECT "
					+ "tapcc.area,"
					+ "tapcc.product_id,"
					+ "tapcc.click_count,"
					+ "tapcc.city_infos,"
					+ "pi.product_name,"
					+ "if(get_json_object(extend_info,'product_status')='0','自营','第三方') product_status "
				+ "FROM tmp_area_product_click_count tapcc "
				+ "JOIN product_info pi "
				+ "ON pi.product_id=tapcc.product_id";
		
		DataFrame df = sqlContext.sql(sql);
		
		df.registerTempTable("tmp_area_fullprod_click_count");
		
	}
	
	/**
	 * 使用Spark SQL开窗函数统计各个区域下top3热门商品
	 * @param sqlContext
	 * @return
	 */
	private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
		
		// 华北、华东、华南、华中、西北、西南、东北
				// A级：华北、华东
				// B级：华南、华中
				// C级：西北、西南
				// D级：东北
				
				// case when
				// 根据多个条件，不同的条件对应不同的值
				// case when then ... when then ... else ... end
		
		String sql ="SELECT "
						+ "area,"
							+ "CASE "
								+ "WHEN area='华北' OR area='华东' THEN 'A级' "
								+ "WHEN area='华南' OR area='华中' THEN 'B级' "
								+ "WHEN area='西北' OR area='西南' THEN 'C级' "
							+ "ELSE 'D级' "
							+ "END area_level,"
						+ "product_id,"
						+ "city_infos,"
						+ "click_count,"
						+ "product_name,"
						+ "product_status "
					+ "FROM("
						+"SELECT "
							+ "area,"
							+ "product_id,"
							+ "click_count,"
							+ "city_infos,"
							+ "product_name,"
							+ "product_status,"
							//使用开窗函数row_number，后面必须跟OVER关键字，在OVER中partition by是按照哪个字段分区
							//然后order by指分组内按照哪个字段进行排序，row_number会为每个组内的记录打一个行号，
							+ "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
						+ "FROM tmp_area_fullprod_click_count) t "
						+ "WHERE rank<=3";
		
		DataFrame df = sqlContext.sql(sql);
		
		return df.javaRDD();
	}
	
	/**
	 * 持久化各区域top3热门商品信息到mysql数据库
	 * 由于数据量比较小，所以先落地为List，然后批量插入数据库
	 * @param taskId
	 * @param productList
	 */
	private static void persistAreaTop3Product(long taskId, List<Row> productList) {
		List<AreaTop3Product> listParams = new ArrayList<AreaTop3Product>();
		AreaTop3Product areaTop3Product = new AreaTop3Product();
		for(Row row: productList) {
			areaTop3Product.setTaskId(taskId);
			areaTop3Product.setArea(row.getString(0));
			areaTop3Product.setAreaLevel(row.getString(1));
			areaTop3Product.setProductId(row.getLong(2));
			areaTop3Product.setCityInfos(row.getString(3));
			areaTop3Product.setClickCount(row.getLong(4));
			areaTop3Product.setProductName(row.getString(5));
			areaTop3Product.setProductStatus(row.getString(6));
			listParams.add(areaTop3Product);
		}
		IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
		areaTop3ProductDAO.insertBatch(listParams);
	}
}
