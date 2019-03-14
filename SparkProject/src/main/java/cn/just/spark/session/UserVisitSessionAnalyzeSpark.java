package cn.just.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;

import cn.just.spark.conf.ConfigurationManager;
import cn.just.spark.constant.Constants;
import cn.just.spark.dao.ISessionAggrSataDAO;
import cn.just.spark.dao.ISessionDetailDAO;
import cn.just.spark.dao.ISessionRandomExtractDAO;
import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.dao.ITop10CategoryDAO;
import cn.just.spark.dao.ITop10CategorySessionDAO;
import cn.just.spark.dao.impl.DAOFactory;
import cn.just.spark.domain.SessionAggrSata;
import cn.just.spark.domain.SessionDetail;
import cn.just.spark.domain.SessionRandomExtract;
import cn.just.spark.domain.Task;
import cn.just.spark.domain.Top10Category;
import cn.just.spark.domain.Top10CategorySession;
import cn.just.spark.test.MockData;
import cn.just.spark.utils.DateUtils;
import cn.just.spark.utils.NumberUtils;
import cn.just.spark.utils.ParamUtils;
import cn.just.spark.utils.SparkUtils;
import cn.just.spark.utils.StringUtils;
import cn.just.spark.utils.ValidUtils;
import scala.Tuple2;

/**
 * 项目第一个模块：
 * 用户访问session分析Spark程序
 * @author shinelon
 * 1、时间范围：起始日期-结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定关键词，那么session就符合条件
 * 7、点击品类：点击多个品类，只要某个session中的任何一个action点击过指定 品类，那么session就符合条件
 *
 */
public class UserVisitSessionAnalyzeSpark {
	
	static final Logger logger=Logger.getLogger(UserVisitSessionAnalyzeSpark.class);
	
	static Boolean local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
	
	public static void main(String[] args) {
		
		//本地测试任务1
//		args=new String[] {"1"};
		//本地测试任务2
//		args=new String[] {"2"};
		
		SparkConf conf=new SparkConf()
				.setAppName(ConfigurationManager.getProperties(Constants.SPARK_APP_NAME_SESSION))
//				.setMaster("local")
				//设置map端内存缓存区大小，默认是32KB
				.set("spark.shuffle.file.buffer", "64")
				//设置reduce端内存聚合占比，默认是executor堆外内存的0.2
				.set("spark.shuffle.memoryFraction", "0.3")
				//减少RDD缓存的内存占比，默认占百分之五十
				.set("spark.storage.memoryFraction", "0.5")
				//性能优化，使用Kryo序列化
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[]{CategorySortKey.class});
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc=new JavaSparkContext(conf);
//		SQLContext sqlContext=getSQLContext(sc.sc());
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
//		mockData(sc, sqlContext);
		
		SparkUtils.mockData(sc, sqlContext);
		
		
		//使用到的JDBC等组件
		ITaskDAO taskDAO=DAOFactory.getTaskDAO();
		
		//执行任务时，首先会从J2EE平台执行spark submit脚本，并且传递参数给shell脚本启动spark作业
		//spark submit脚本会将参数传递给spark的main函数，也就是spark作业的main函数的args参数
		Long taskId=ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
		
		Task task=taskDAO.findById(taskId);
		
		JSONObject jsonObject=JSONObject.parseObject(task.getTaskParam());
		
		//按照时间范围从user_visit_action过筛选信息
		JavaRDD<Row> actionRDD=getVisitActionByDateRange(sqlContext, jsonObject);
		
		//转换数据格式为<sessionId,Row>的格式
		JavaPairRDD<String, Row> sessionIdRowRDD=sessionId2actionRDD(actionRDD);
		
		/**
		 * 性能优化
		 * 第一选择：MEMORY_ONLY（纯内存）
		 * 第二选择：MEMORY_ONLY_SER（纯内存序列化）
		 * 第三选择：MEMORY_AND_DISK（内存+磁盘）
		 * 第四选择：MEMORY_AND_DISK_SER（内存+磁盘+序列化）
		 * 第五选择：DISK_ONLY（纯磁盘）
		 * 第六选择：MEMORY_ONLY_SER（纯磁盘+序列化）
		 * 后续选择：选择以_2为后缀的是使用高可靠的双备份机制，比如MEMORY_ONLY_SER_2
		 */
		//持久化到纯内存
		sessionIdRowRDD=sessionIdRowRDD.persist(StorageLevel.MEMORY_ONLY());
		
		StorageLevel.MEMORY_ONLY_SER();
		
		//然后可以将信息按照sessionId进行groupByKey操作可以得到session粒度的信息
		//然后将格式转换为<userId,partAggrInfo>，partAggrInfo包括上面提到的搜索词searchKeyWord和
		//clickCategoryInfo，然后按照userId与用户信息进行join操作，最终得到所需要的聚合数据
		//最终的数据格式是<sessionId,fullAggrInfo>
		//		JavaPairRDD<String, String> fullAggrInfo=getAggrInfo(sqlContext, actionRDD);
		/**
		 * 代码重构：
		 * 由于在getAggrInfo函数中actionRDD会生成sessionIdRowRDD，那么我们可以重构代码
		 * 将actionRDD形参直接换成sessionIdRowRDD，减少了RDD之间重新计算浪费的时间
		 */
		JavaPairRDD<String, String> fullAggrInfo=getAggrInfo(sc,sqlContext, sessionIdRowRDD);
		
		//定义自定义accumulator
		//存储session聚合步长或者时长
		Accumulator<String> sessionAggrSataAccumulator=new Accumulator<String>(
				"",new SessionAggrSataAccumulator());
		//按照条件进行筛选
		JavaPairRDD<String, String> filterAggrInfo=filterSessionAndAggrInfoRDD(
				fullAggrInfo, jsonObject,sessionAggrSataAccumulator);
		
		/**
		 * 性能优化:
		 * 持久化数据
		 */
		filterAggrInfo=filterAggrInfo.persist(StorageLevel.MEMORY_ONLY());
		
		
		/**
		 * 在这里将结果写入mysql之前必须触发一个action操作，然后它才会真正将上述操作作为一个job开始执行
		 * 不然sessionAggrSataAccumulator变量的值是空值
		 */
		
		/**
		 * 在这之前说到在向mysql中写入数据的时候，必须有一个action操作来执行job，正好我们第二个功能随机抽取session的功能
		 * 用到了countByKey算子是一个action操作，因此我们可以写入mysql之前实现随机抽取的功能触发action操作
		 */
		System.out.println(filterAggrInfo.count());
		//计算session访问时长和访问步长的占比并且将其写入mysql数据库
		accumulateAggrSataPersist(sessionAggrSataAccumulator.value(),task.getTaskId());
		
		//随机抽取session模块
		randomExtractSession(sc,task.getTaskId(),filterAggrInfo,sessionIdRowRDD);
		
		//生成公共模块，获取符合条件的session明细数据
		JavaPairRDD<String, Row> session2DetailRDD=getFilterCommonRDD(filterAggrInfo, sessionIdRowRDD);
		
		/**
		 * 持久化数据
		 */
		session2DetailRDD=session2DetailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		//获取top10热门品类
		List<Tuple2<CategorySortKey, String>> top10CatehoryList=top10Category(
							task.getTaskId(),session2DetailRDD);
		
		//获取每个品类对应的top10session
		top10Session(task.getTaskId(),sc,top10CatehoryList,session2DetailRDD);
		
		sc.stop();
	}


	/**
	 * 获取sqlContext
	 * 需要判断是否在本地模式下，在本地模式下，使用SQLContext进行模拟
	 * 否则使用HiveContext
	 * @param sc
	 * @return
	 */
	public static SQLContext getSQLContext(SparkContext sc) {
		
		if(local) {
			SQLContext sqlContext=new SQLContext(sc);
			return sqlContext;
		}else {
			HiveContext hiveContext=new HiveContext(sc);
			return hiveContext;
		}
	}
	
	/**
	 * 测试模拟数据，只有在本地模式下使用模拟数据进行测试
	 * @param sc JavaSparkContext
	 * @param sqlContext
	 */
	public static void mockData(JavaSparkContext sc,SQLContext sqlContext) {
		if(local)
			MockData.mock(sc, sqlContext);
	}
	
	/**
	 * 按照时间范围从hive表中来筛选信息
	 * @param sqlContext
	 * @param jsonObject 封装为json数据格式的task参数
	 * @return
	 */
	public static JavaRDD<Row> getVisitActionByDateRange(SQLContext sqlContext,JSONObject jsonObject){
		
		String startDate=ParamUtils.getParam(jsonObject, Constants.TASK_START_DATE);
		String endDate=ParamUtils.getParam(jsonObject, Constants.TASK_END_DATE);
		
		String sql="select * from user_visit_action "
				+ "where date>='"+startDate
				+"' and date<='"+endDate+"'";
		DataFrame actionDF=sqlContext.sql(sql);
		
		/**
		 * 由于使用SparkSQL不能设置并行度，系统会根据hive表（hdfs block块数量）自己设置并行度
		 * 因此，有可能会导致task数量太少，速度太慢，因此我们可以在SparkSQL处理之后使用repartition算子
		 * 重新分区，增加并行度
		 */
		//local模式会自动优化，当在集群中运行时值得考虑设置并行度
//		return actionDF.javaRDD().repartition(1000);
		
		return actionDF.javaRDD();
	}
	
	
	
	public static JavaPairRDD<String, String> getAggrInfo(
			JavaSparkContext sc,
			SQLContext sqlContext,
			JavaPairRDD<String,
			Row> sessionIdRowRDD){
		
		/**
		 * 代码重构
		 */
		//转换为<sessionId,Row>格式数据 
//		JavaPairRDD<String, Row> rdd2PairRDD=actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//
//			@Override
//			public Tuple2<String, Row> call(Row row) throws Exception {
//				String sessionId=row.getString(2);
//				return new Tuple2<String, Row>(sessionId, row);
//			}
//		});
		
		//按照sessionId进行分组,然后得到session粒度的数据
		JavaPairRDD<String, Iterable<Row>> sessionRDD=sessionIdRowRDD.groupByKey();
		
		//转换为<userId,partAggrInfo>数据格式，partAggrInfo包括sessionId,searchKeyWord和clickCategory
		JavaPairRDD<Long, String> sessionId2PartAggrInfo=sessionRDD.mapToPair(
				
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> session) throws Exception {
				String sessionId=session._1;
				Iterator<Row> it=session._2.iterator();
				
				StringBuffer searchKeyWordBuffer=new StringBuffer("");
				StringBuffer clickCategoryBuffer=new StringBuffer("");
				Long userId=null;
				
				/**
				 * 代码重构，计算session访问时长和访问步长
				 */
				Date startTime=null;
				Date endTime=null;
				//Session访问步长
				int stepLength=0;
				
				while(it.hasNext()) {
					Row row=it.next();
					String searchKeyWord=row.getString(5);
					Long clickCategoryId=row.getLong(6);
					userId=row.getLong(1);
					//这里需要注意，searchKeyWord字段和clickCategoryId字段不可能在同一条记录中同时存在
					//因为用户搜索了东西就不没有点击字段，如果点击了东西就没有搜索东西
					//所以这里需要判断，字段不能为空并且当前的字符串中不包含
					if(StringUtils.isNotEmpty(searchKeyWord)) {
						if(!searchKeyWordBuffer.toString().contains(searchKeyWord)) {
							searchKeyWordBuffer.append(searchKeyWord+",");
						}
					}
					if(clickCategoryId!=null) {
						if(!clickCategoryBuffer.toString().contains(String.valueOf(clickCategoryId))) {
							clickCategoryBuffer.append(clickCategoryId+",");
						}
					}
					Date actionTime=DateUtils.parseTime(row.getString(4));
					if(startTime==null) {
						startTime=actionTime;
					}
					if(endTime==null) {
						endTime=actionTime;
					}
					if(actionTime.before(startTime)) {
						startTime=actionTime;
					}
					if(actionTime.after(endTime)) {
						endTime=actionTime;
					}
					//计算session访问步长
					stepLength++;
					
				}
				
				//计算session访问时长(秒)
				long visitLength=(endTime.getTime()-startTime.getTime())/1000;
				
				//去掉两边的逗号
				String searchKeyWordInfo=StringUtils.trimComma(searchKeyWordBuffer.toString());
				String clickCategoryInfo=StringUtils.trimComma(clickCategoryBuffer.toString());
				
//				System.out.println("============"+startTime);
				
				//转换为固定的格式，key=value|key=value
				String partAggrInfo=Constants.FIELD_SESSION_ID+"="+sessionId+"|"
						+Constants.FIELD_SEARCH_KEY_WORD+"="+searchKeyWordInfo+"|"		//这里判断searchKeyWordInfo和clickCategoryInfo是否为空
						+Constants.FIELD_CLICK_CATEGORY_ID+"="+clickCategoryInfo+"|"
						+Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
						+Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
						+Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime)+"|";
				
				if(partAggrInfo.endsWith("|")) {
					partAggrInfo=partAggrInfo.substring(0,partAggrInfo.length()-1);
				}
				
				return new Tuple2<Long, String>(userId, partAggrInfo);
			}
		});
		
		//查询所有用户的信息以便后面用于join 操作
		String sql="select * from user_info";
		JavaRDD<Row> userRDD=sqlContext.sql(sql).javaRDD();
		
		//转换为<userId,Row>格式的数据格式
		JavaPairRDD<Long, Row> userPairRDD=userRDD.mapToPair(
				
				new PairFunction<Row, Long, Row>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				Long userId=row.getLong(0);
				return new Tuple2<Long, Row>(userId, row);
			}
		});
		
		/**
		 * 这里可将 reduce join转换为map join防止数据倾斜
		 * 因为sessionId2PartAggrInfo数据量相比userPairRDD数据量比较小
		 * 因此可以采用这种方法
		 */
		
		//session粒度的信息与user用户的信息进行join操作
		JavaPairRDD<Long,Tuple2<String, Row>> allInfo=sessionId2PartAggrInfo.join(userPairRDD);
		
		//将join的数据格式<userId,<partAggrInfo,userInfo>>转换为<sessionId,fullAggrInfo>的数据格式
		JavaPairRDD<String, String> allAggrInfo=allInfo.mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

				private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> allInfo) throws Exception {
				String partAggrInfo=allInfo._2._1;
				
				//通过工具类分割特定的分隔符来获取sessionId
				String sessionId=StringUtils.getFieldFromConcatString(
						partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);		//将“|”进行了转义操作
				
				Row userInfo=allInfo._2._2;
				Integer age=userInfo.getInt(3);
				String professional=userInfo.getString(4);
				String city=userInfo.getString(5);
				String sex=userInfo.getString(6);
				
				String fullAggrInfo=partAggrInfo+"|"
								+	Constants.FIELD_AGE+"="+age+"|"
								+	Constants.FIELD_PROFESSIONAL+"="+professional+"|"
								+	Constants.FIELD_CITY+"="+city+"|"
								+	Constants.FIELD_SEX+"="+sex;
				return new Tuple2<String, String>(sessionId,fullAggrInfo);
				
			}
		});
		
		
		/**
		 * reduce join 转换为 map join
		 */
//		List<Tuple2<Long, Row>> userInfos=userPairRDD.collect();
//		//1.将小的RDD作为一个广播变量
//		final Broadcast<List<Tuple2<Long,Row>>> userInfoCast=sc.broadcast(userInfos);
//		//2.使用mapToPair函数
//		JavaPairRDD<String, String> allAggrInfo=sessionId2PartAggrInfo.mapToPair(
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String,String> call(Tuple2<Long, String> tuple) throws Exception {
//				//获取值
//				List<Tuple2<Long,Row>> userInfos=userInfoCast.value();
//				//放入缓存中，即executor中的BlockManager中，所以必须确保你的内存足够来存放这个 小RDD
//				Map<Long,Row> userInfoMap=new HashMap<Long, Row>();
//				for(Tuple2<Long,Row> userInfo:userInfos) {
//					userInfoMap.put(userInfo._1, userInfo._2);
//				}
//				//获取当前用户对应的信息
//				String partAggrInfo = tuple._2;
//				Row userInfoRow = userInfoMap.get(tuple._1);
//				
//				String sessionid = StringUtils.getFieldFromConcatString(
//						partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//				
//				int age = userInfoRow.getInt(3);
//				String professional = userInfoRow.getString(4);
//				String city = userInfoRow.getString(5);
//				String sex = userInfoRow.getString(6);
//				
//				String fullAggrInfo = partAggrInfo + "|"
//						+ Constants.FIELD_AGE + "=" + age + "|"
//						+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//						+ Constants.FIELD_CITY + "=" + city + "|"
//						+ Constants.FIELD_SEX + "=" + sex;
//				
//				return new Tuple2<String, String>(sessionid, fullAggrInfo);
//			}
//		});
		
		/**
		 * 解决数据倾斜：
		 * sample随机取样倾斜key进行两次join：
		 * 1.随机采样一部分RDD
		 * 2.按照key出现的次数排序，选取出现次数最多的几个key
		 * 3.分别用抽取的key和剩余的key和其他RDD进行join操作
		 * 4.将两个join的key进行union操作
		 * 
		 * 优化方案：
		 * 
		 * 将那个抽取的key加一个100以内的随机数前缀进行打散操作，使得均匀分布
		 * 然后再进行join操作，之后去掉前缀。
		 * 
		 * 此处使用该方法解决数据倾斜的原因：
		 * sessionId2PartAggrInfo中key的数量不是很多，但是每个key对应的数据量很大，
		 * 因此该方法适合一个RDD的key很少的情况，但是数据量很大需要join的操作。
		 */
		
//		JavaPairRDD<Long,String> sampleSessionRDD = sessionId2PartAggrInfo.sample(false, 0.1 ,9);
//		
//		// 转化为<key,1>的格式，然后按照key出现的次数进行排序操作
//		JavaPairRDD<Long,Long> mapSampleSessionRDD = sampleSessionRDD.mapToPair(
//				new PairFunction<Tuple2<Long,String>, Long, Long>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) throws Exception {
//				return new Tuple2<Long, Long>(tuple._1, 1L);
//			}
//		});
//		
//		JavaPairRDD<Long,Long> countSampleSessionRDD = mapSampleSessionRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Long call(Long v1, Long v2) throws Exception {
//				return v1+v2;
//			}
//		});
//		
//		//转换为<1,key>的格式进行排序
//		JavaPairRDD<Long,Long> reserveSampleSessionRDD = countSampleSessionRDD.mapToPair(
//				new PairFunction<Tuple2<Long,Long>, Long, Long>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<Long , Long> call(Tuple2<Long, Long> tuple) throws Exception {
//				return new Tuple2<Long, Long>(tuple._2, tuple._1);
//			}
//		});
//		
//		//获取发生倾斜的数据的key
//		final Long inclineSessionKey = reserveSampleSessionRDD.sortByKey(false).take(1).get(0)._2;
//		
//		//发生数据倾斜的RDD
//		JavaPairRDD<Long, String> inclineSessionRDD = sessionId2PartAggrInfo.filter(
//				
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//				return tuple._1.equals(inclineSessionKey);
//			}
//		});
//		//其他普通的RDD
//		JavaPairRDD<Long, String> commonSessionRDD = sessionId2PartAggrInfo.filter(
//						
//						new Function<Tuple2<Long,String>, Boolean>() {
//		
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return !tuple._1.equals(inclineSessionKey);
//					}
//				});
//		
//		//从另一个需要join的RDD中取出发生数据倾斜的key对应的数据，进行加前缀打散操作
//		JavaPairRDD<Long,Row> inclineUserInfoRDD = userPairRDD.filter(
//				
//				new Function<Tuple2<Long,Row>, Boolean>() {
//
//				private static final long serialVersionUID = 1L;
//	
//				@Override
//				public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
//					return tuple._1.equals(inclineSessionKey);
//				}
//			});
//		
//		//加前缀打散RDD
//		JavaPairRDD<String,Row> perixUserInfoRDD = inclineUserInfoRDD.flatMapToPair(
//				
//				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
//				Random random = new Random();
//				int perix = random.nextInt(100);
//				List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String,Row>>();
//				list.add(new Tuple2<String, Row>(perix+"_"+tuple._1, tuple._2));
//				return list;
//			}
//		});
//		
//		JavaPairRDD<String,String> preixSessionRDD = inclineSessionRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String,String>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//				Random random = new Random();
//				int perix = random.nextInt(100);
//				return new Tuple2<String, String>(perix+"_"+tuple._1, tuple._2);
//			}
//		});
//		
//		//对于倾斜的那个key进行join操作然后去掉前缀
//		JavaPairRDD<Long,Tuple2<String,Row>> joinRDD1 = preixSessionRDD.join(perixUserInfoRDD)
//						.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String,Row>>() {
//
//							private static final long serialVersionUID = 1L;
//
//							@Override
//							public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple)
//									throws Exception {
//								Long sessionId = Long.valueOf(tuple._1.split("_")[1]);
//								return new Tuple2<Long, Tuple2<String, Row>>(sessionId,tuple._2);
//							}
//						});
//		
//		JavaPairRDD<Long,Tuple2<String,Row>> joinRDD2 = commonSessionRDD.join(userPairRDD);
//		//合并join
//		JavaPairRDD<Long,Tuple2<String,Row>> unionJoinRDD = joinRDD1.union(joinRDD2);
//		
//		JavaPairRDD<String, String> allAggrInfo = unionJoinRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
//
//				private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> allInfo) throws Exception {
//				String partAggrInfo=allInfo._2._1;
//				
//				//通过工具类分割特定的分隔符来获取sessionId
//				String sessionId=StringUtils.getFieldFromConcatString(
//						partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);		//将“|”进行了转义操作
//				
//				Row userInfo=allInfo._2._2;
//				Integer age=userInfo.getInt(3);
//				String professional=userInfo.getString(4);
//				String city=userInfo.getString(5);
//				String sex=userInfo.getString(6);
//				
//				String fullAggrInfo=partAggrInfo+"|"
//								+	Constants.FIELD_AGE+"="+age+"|"
//								+	Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//								+	Constants.FIELD_CITY+"="+city+"|"
//								+	Constants.FIELD_SEX+"="+sex;
//				return new Tuple2<String, String>(sessionId,fullAggrInfo);
//				
//			}
//		});
//		
		
		/**
		 * 解决数据倾斜：
		 * 使用随机数以及扩容表join：
		 * 对一个RDD进行随机数前缀扩容
		 * 另一个RDD前缀打散
		 * 然后两个RDD进行join操作
		 */
		
//		JavaPairRDD<String,Row> expandedUserInfoRDD = userPairRDD.flatMapToPair(
//				
//				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
//				List<Tuple2<String,Row>> list = new ArrayList<Tuple2<String,Row>>();
//				for(int i=0;i<10;i++) {
//					list.add(new Tuple2<String,Row>(i+"_"+tuple._1,tuple._2));
//				}
//				return list;
//			}
//		});
//		
//		JavaPairRDD<String,String> prefixSessionRDD = sessionId2PartAggrInfo.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String,String>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
//				Random random = new Random();
//				int prefix = random.nextInt(10);
//				return new Tuple2<String,String>(prefix+"_"+tuple._1,tuple._2);
//			}
//		});
//		
//		JavaPairRDD<String,Tuple2<String,Row>> joinRDD = prefixSessionRDD.join(expandedUserInfoRDD);
//		
//		JavaPairRDD<Long,Tuple2<String,Row>> mapJoinRDD = joinRDD.mapToPair(
//				
//				new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String,Row>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//				return new Tuple2<Long, Tuple2<String, Row>>(Long.valueOf(tuple._1.split("_")[1]),tuple._2);
//			}
//		});
//		
//		JavaPairRDD<String, String> allAggrInfo = mapJoinRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
//
//				private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> allInfo) throws Exception {
//				String partAggrInfo=allInfo._2._1;
//				
//				//通过工具类分割特定的分隔符来获取sessionId
//				String sessionId=StringUtils.getFieldFromConcatString(
//						partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);		//将“|”进行了转义操作
//				
//				Row userInfo=allInfo._2._2;
//				Integer age=userInfo.getInt(3);
//				String professional=userInfo.getString(4);
//				String city=userInfo.getString(5);
//				String sex=userInfo.getString(6);
//				
//				String fullAggrInfo=partAggrInfo+"|"
//								+	Constants.FIELD_AGE+"="+age+"|"
//								+	Constants.FIELD_PROFESSIONAL+"="+professional+"|"
//								+	Constants.FIELD_CITY+"="+city+"|"
//								+	Constants.FIELD_SEX+"="+sex;
//				return new Tuple2<String, String>(sessionId,fullAggrInfo);
//				
//			}
//		});
		
		return allAggrInfo;
	}
	
	
	/**
	 * RDD的格式转换
	 * 转换为<sessionid,Row>的格式
	 * @param actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, Row> sessionId2actionRDD(JavaRDD<Row> actionRDD) {
		
		/**
		 * 算子优化：
		 * 使用MapPartition代替Map算子
		 * Map算子是将RDD的Partition中的数据 一条条的加载入内存，传入function中
		 * 而MapPartition操作是将RDD的Partition中的数据全部加载入内存，传入function中，
		 * 但是这样有可能出现OOM，因此使用在数据量较小的RDD中
		 */
//		 JavaPairRDD<String, Row> pairRDD=actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Row> call(Row row) throws Exception {
//				String sessionId=row.getString(2);
//				return new Tuple2<String, Row>(sessionId, row);
//			}
//		});
		
		JavaPairRDD<String, Row> pairRDD=actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Row> it) throws Exception {
				List<Tuple2<String,Row>> list=new ArrayList<Tuple2<String,Row>>();
				while(it.hasNext()) {
					Row row=it.next();
					list.add(new Tuple2<String, Row>(row.getString(2),row));
				}
				return list;
			}
		});
		
		 return pairRDD;
	}
	
	
	
	
	
	/**
	 * 随机抽取session 100条
	 * 实现思路：
	 * 首先将数据格式映射为<yyyy-MM-dd_HH,AggrInfo>的格式，然后使用countByKey计算出每天每个小时
	 * 的session数量，接着按照百分比计算出每天每个小时需要随机抽取的session数量，然后生成相应数量的
	 * 随机数作为每个小时需要抽取session的索引
	 * 然后使用groupByKey分组，遍历每天每个小时的session，当遇到对应要抽取的索引的时候就进行随机抽取，
	 * 最终抽取到相应数量的session个数
	 * @param filterAggrInfo
	 * @param sessionIdRowRDD 
	 */
	private static void randomExtractSession(
											JavaSparkContext sc,
											final long taskId,
											JavaPairRDD<String, String> filterAggrInfo, 
											JavaPairRDD<String,Row> sessionIdRowRDD) {

		  JavaPairRDD<String, String> time2SessionRDD=filterAggrInfo.mapToPair(
				
				new PairFunction<Tuple2<String,String>, String, String>() {

					private static final long serialVersionUID = 1L;
					
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				//转换为相应的数据格式<yyyy-MM-dd_HH,AggrInfo>的格式
				//因为随机抽取的数据要写入session_random_extract表中，因此value必须是AggrInfo
				String fullAggrInfo=tuple._2;
				
//				System.out.println(fullAggrInfo);
				
				String startTime=StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME);
				//截取年月日时
				startTime=DateUtils.getDateHour(startTime);
				return new Tuple2<String, String>(startTime, fullAggrInfo);
			}
		});
		//计算每小时session的数量<yyyy-MM-dd_HH,count>
		Map<String,Object> sessionCount=time2SessionRDD.countByKey();
		
		System.out.println("sessionCount:"+sessionCount.size());
		
		/**
		 * 实现按照时间比例随机抽取session
		 */
		
		//转换数据格式为<date,<hour,count>>
		Map<String,Map<String,Long>> sessionCountPerDay=new HashMap<String, Map<String,Long>>();
		
			
		for(Map.Entry<String, Object> sessionEntry:sessionCount.entrySet()) {
			String date=sessionEntry.getKey();
			String perDate=date.split("_")[0];
			String hour=date.split("_")[1];
			//每小时的session数
			long countPerHour=Long.valueOf(String.valueOf(sessionEntry.getValue()));
			
			Map<String,Long> sessionCountPerHour=sessionCountPerDay.get(perDate);
			if(sessionCountPerHour==null) {
				sessionCountPerHour=new HashMap<String, Long>();
				sessionCountPerDay.put(perDate, sessionCountPerHour);
			}
			sessionCountPerHour.put(hour, countPerHour);
			
		}
		
		//平均每天要抽取的session个数
		int randomExtractNumberPerDay = 100 / sessionCountPerDay.size();
		
//		System.out.println("sessionCountPerDay.size():"+sessionCountPerDay.size());
		
//		System.out.println("平均每天的session："+randomExtractNumberPerDay);
		
		//将数据格式转换为<date,<hour,list(index1,index2...)>>的格式
		/**
		 * 因为默认map会被拷贝一份副本到每一个task中，因此对性能极为不佳，所以我们可以使用
		 * 广播变量来进行优化
		 */
		Map<String,Map<String,List<Integer>>> sessionCountIndex=
				new HashMap<String,Map<String,List<Integer>>>();
		
		//初始化随机变量
		Random random=new Random();
		
		for(Map.Entry<String,Map<String,Long>> sessionCountIndexPerHour : sessionCountPerDay.entrySet()) {
			Map<String,Long> countPerHour=sessionCountIndexPerHour.getValue();
			//当天session总数
			long totalSessionCount=0l;
			//计算当天session的总数
			for(long count:countPerHour.values()) {
				totalSessionCount+=count;
			}
			
			
			System.out.println("当天总session数："+totalSessionCount);
			
			String day=sessionCountIndexPerHour.getKey();
			
			Map<String, List<Integer>> indexMap=sessionCountIndex.get(day);
			if(indexMap==null) {
				indexMap=new HashMap<String, List<Integer>>();
				sessionCountIndex.put(day, indexMap);
			}
			
			//遍历每个小时
			for(Map.Entry<String, Long> hourCount:countPerHour.entrySet()) {
				String hour=hourCount.getKey();
				//每个小时的session个数
				long count=hourCount.getValue();
				
				//计算每个小时要随机抽取的session个数
				//（每个小时的session个数/当天的session总个数）*每天平均要抽取的session数目
				int randomExtractNumberPerHour=(int)(((double)count/(double)totalSessionCount)*randomExtractNumberPerDay);
				
				System.out.println("每个小时随机抽取的session数："+randomExtractNumberPerHour);
				
				//如果每个小时随机抽取的session个数大于每个小时的session个数，则每个小时随机抽取的个数等于该小时的session个数
				if(randomExtractNumberPerHour>count) {
					randomExtractNumberPerHour=(int)count;
				}
				
				List<Integer> indexList=indexMap.get(hour);
				if(indexList==null) {
					indexList=new ArrayList<Integer>();
					indexMap.put(hour, indexList);
				}
				
				//计算每天要随机抽取的索引
				for(int i=0;i<randomExtractNumberPerHour;i++) {
					int index=random.nextInt((int)count);
					//防止重复
					while(indexList.contains(index)) {
						index=random.nextInt((int)count);
					}
					indexList.add(index);
				}
				System.out.println("indexList:"+indexList.size());
				
			}
		}
		
		/**
		 * 性能优化：
		 * 将map创建为广播变量
		 */
		final Broadcast<Map<String,Map<String,List<Integer>>>> sessionCountIndexBroadcast=
				sc.broadcast(sessionCountIndex);
		
		/**
		 * 根据随机抽取的索引数随机进行抽取，并且写入session_random_extract表中
		 * 并且将索引放入list中返回，list中返回tuple(sessionId,sessionid)以便后面进行join操作，
		 * 将数据写入session_detail表中
		 */
		
		//每个小时的sessionInfo
		JavaPairRDD<String, Iterable<String>> date2AggrInfoRDD=time2SessionRDD.groupByKey();
		
		//数据格式<sessionId,sessionId>
		JavaPairRDD<String, String> sessioIdRDD=date2AggrInfoRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				//存放抽取的sessionId，使用pair的格式，方便后面进行join操作
				List<Tuple2<String, String>> sessionIdList=new ArrayList<Tuple2<String, String>>();
				String dateHour=tuple._1;
				String date=dateHour.split("_")[0];
				String hour=dateHour.split("_")[1];
				
				/**
				 * 通过广播变量来获取
				 * 使用value方法或者getValue方法来获取
				 */
				Map<String,Map<String,List<Integer>>> sessionCountIndexValue=sessionCountIndexBroadcast.value();
				
				//存放随机生成的session索引
				List<Integer> randomExtractIndex=sessionCountIndexValue.get(date).get(hour);
				
				System.out.println("索引个数："+randomExtractIndex.size());
				
				//索引计数器
				int index=0;
				
				//实体封装类
				SessionRandomExtract sessionRandomExtract=new SessionRandomExtract();
				
				ISessionRandomExtractDAO sessionRandomExtractDAOImpl=DAOFactory.getSessionRandomExtractDAO();
				
				Iterator<String> aggrInfoIterator=tuple._2.iterator();
				while(aggrInfoIterator.hasNext()) {
					
					String aggrInfo=aggrInfoIterator.next();
					
					//如果包含要随机抽取的索引
					//写入session_random_extract表中并且放入list
					if(randomExtractIndex.contains(index)) {
						
						sessionRandomExtract.setTaskId(taskId);
						String sessionId=StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						sessionRandomExtract.setSessionId(sessionId);
						sessionRandomExtract.setStartTime(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME));
						sessionRandomExtract.setSearchKeywords(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SEARCH_KEY_WORD));
						sessionRandomExtract.setClickCategoryIds(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_ID));
						//写入mysql数据库中
						sessionRandomExtractDAOImpl.insert(sessionRandomExtract);
						//放入list中
						sessionIdList.add(new Tuple2<String,String>(sessionId, sessionId));
					}
					index++;
				}
//				System.out.println("sessionIdList:"+sessionIdList.size());
				return sessionIdList;
			}
		});
		
		/**
		 * 进行join操作，并且将session明细数据放入数据库中
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionIdAndDetailRDD=sessioIdRDD.join(sessionIdRowRDD);
		/**
		 * 使用foreachPartition操作代替foreach操作
		 */
//		sessionIdAndDetailRDD.foreach(
//				new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
//			
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//				String sessionId=tuple._1;
//				Row row=tuple._2._2;
//				SessionDetail sessionDetail=new SessionDetail();
//				sessionDetail.setTaskId(taskId);
//				sessionDetail.setUserId(row.getLong(1));
//				sessionDetail.setSessionId(row.getString(2));
//				sessionDetail.setPageId(row.getLong(3));
//				sessionDetail.setActionTime(row.getString(4));
//				sessionDetail.setSearchKeyword(row.getString(5));
//				sessionDetail.setClickCategoryId(row.getLong(6));
//				sessionDetail.setClickProductId(row.getLong(7));
//				sessionDetail.setOrderCategoryIds(row.getString(8));
//				sessionDetail.setOrderProductIds(row.getString(9));
//				sessionDetail.setPayCategoryIds(row.getString(10));
//				sessionDetail.setPayProductIds(row.getString(11));
//				//从DAO工厂获取DAO接口
//				ISessionDetailDAO sessionDetailDAOImpl=DAOFactory.getSessionDetailDAO();
//				//插入数据库
//				sessionDetailDAOImpl.insert(sessionDetail);
//			}
//		});
		
		sessionIdAndDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Tuple2<String,Row>>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> it) throws Exception {
				List<SessionDetail> list=new ArrayList<SessionDetail>();
				while(it.hasNext()) {
					Tuple2<String, Tuple2<String,Row>> tuple=it.next();
					String sessionId=tuple._1;
					Row row=tuple._2._2;
					SessionDetail sessionDetail=new SessionDetail();
					sessionDetail.setTaskId(taskId);
					sessionDetail.setUserId(row.getLong(1));
					sessionDetail.setSessionId(row.getString(2));
					sessionDetail.setPageId(row.getLong(3));
					sessionDetail.setActionTime(row.getString(4));
					sessionDetail.setSearchKeyword(row.getString(5));
					sessionDetail.setClickCategoryId(row.getLong(6));
					sessionDetail.setClickProductId(row.getLong(7));
					sessionDetail.setOrderCategoryIds(row.getString(8));
					sessionDetail.setOrderProductIds(row.getString(9));
					sessionDetail.setPayCategoryIds(row.getString(10));
					sessionDetail.setPayProductIds(row.getString(11));
					list.add(sessionDetail);
				}
//				从DAO工厂获取DAO接口
				ISessionDetailDAO sessionDetailDAOImpl=DAOFactory.getSessionDetailDAO();
				sessionDetailDAOImpl.executeBatch(list);
			}
		});
	}
	
	
	
	
	
	
	/**
	 * 按照条件筛选聚合数据
	 * @param fullAggrInfo
	 * @param jsonArray
	 * @return
	 */
	public static JavaPairRDD<String, String> filterSessionAndAggrInfoRDD(
									JavaPairRDD<String, String> fullAggrInfo,
									JSONObject jsonObject,
									final Accumulator<String> sessionAggrSataAccumulator){
		//拼接筛选参数
		String startAge=ParamUtils.getParam(jsonObject, Constants.TASK_START_AGE);
		String endAge=ParamUtils.getParam(jsonObject, Constants.TASK_END_AGE);
		String professionals=ParamUtils.getParam(jsonObject, Constants.TASK_PROFESSIONALS);
		String city=ParamUtils.getParam(jsonObject, Constants.TASK_CITY);
		String sex=ParamUtils.getParam(jsonObject, Constants.TASK_SEX);
		String keyWords=ParamUtils.getParam(jsonObject, Constants.TASK_KEY_WORD);
		String clickCategoryId=ParamUtils.getParam(jsonObject, Constants.TASK_CLICK_CATEGORY_ID);
		
		logger.info("===============================startAge:"+startAge+"   endAge:"+endAge);
		
		String _parameter=(startAge!=null?Constants.TASK_START_AGE+"="+startAge+"|":"")
					+	  (endAge!=null?Constants.TASK_END_AGE+"="+endAge+"|":"")
					+	  (professionals!=null?Constants.TASK_PROFESSIONALS+"="+professionals+"|":"")
					+	  (city!=null?Constants.TASK_CITY+"="+city+"|":"")
					+	  (sex!=null?Constants.TASK_SEX+"="+sex+"|":"")
					+	  (keyWords!=null?Constants.TASK_KEY_WORD+"="+keyWords+"|":"")		
					+	  (clickCategoryId!=null?Constants.TASK_CLICK_CATEGORY_ID+"="+clickCategoryId+"|":"");
		
		if(_parameter.endsWith("\\|")) {
			_parameter=_parameter.substring(0,_parameter.length()-1);
		}
		final String parameter=_parameter;
		
		
		JavaPairRDD<String, String> filterAggrInfo=fullAggrInfo.filter(
				
				new Function<Tuple2<String,String>, Boolean>() {
			
					private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				
				String fullAggrInfo=tuple._2;
				
				//按照年龄段进行筛选，只要聚合的session数据fullAggrInfo中有一个符合parameter指定年龄段的条件
				//则返回true，否则返回false
				if(!ValidUtils.between(fullAggrInfo, Constants.FIELD_AGE, 
						parameter, Constants.TASK_START_AGE, Constants.TASK_END_AGE)) {
					return false;
				}
				//按照用户的职业进行筛选
				//比如session聚合数据中用户的职业是老师，学生 ，会计等
				//只要筛选的条件中上面有一个符合就返回true，否则false
				if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_PROFESSIONAL,
						parameter, Constants.TASK_PROFESSIONALS)) {
					return false;
				}
				//按照城市进行筛选
				if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_CITY,
						parameter, Constants.TASK_CITY)) {
					return false;
				}
				//按照性别进行筛选
				if(!ValidUtils.equal(fullAggrInfo, Constants.FIELD_SEX,
						parameter,Constants.TASK_SEX)) {
					return false;
				}
				//按照搜索的关键字进行筛选，比如session聚合数据中用户搜索过鱼类，饮料等商品
				//那么筛选的条件为火腿肠，鱼类，那么session中搜索的关键字有任何一个与筛选条件的任何一个关键字相等
				//则返回true，否则返回false
				if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_SEARCH_KEY_WORD,
						parameter, Constants.TASK_KEY_WORD)) {
					return false;
				}
				//按照用户点击的品类进行筛选
				if(!ValidUtils.in(fullAggrInfo, Constants.FIELD_CLICK_CATEGORY_ID,
						parameter, Constants.TASK_CLICK_CATEGORY_ID)) {
					return false;
				}
				
				/**
				 * 经过上面过滤后对符合条件的session进行聚合统计
				 */
				//计算session数量
				sessionAggrSataAccumulator.add(Constants.SESSION_COUNT);

//				System.out.println(fullAggrInfo);
				
				//从过滤后符合条件的聚合信息中提取出访问时长和访问步长
				long visitLength=Long.valueOf(StringUtils.getFieldFromConcatString(
						fullAggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				long stepLength=Long.valueOf(StringUtils.getFieldFromConcatString(
						fullAggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
				
				accumulateVisitLength(visitLength);
				accumulateStepLength(stepLength);
				
				return true;
			}
			/**
			 * 计算符合条件的访问步长
			 * @param stepLength
			 */
			private void accumulateStepLength(long stepLength) {
				if(stepLength>=1&&stepLength<=3) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_1_3);
				}else if(stepLength>=4&&stepLength<=6) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_4_6);
				}else if(stepLength>=7&&stepLength<=9) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_7_9);
				}else if(stepLength>=10&&stepLength<=30) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_10_30);
				}else if(stepLength>30&&stepLength<=60) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_30_60);
				}else if(stepLength>60) {
					sessionAggrSataAccumulator.add(Constants.STEP_PERIOD_60);
				}
			}
			/**
			 * 计算符合条件的访问时长
			 * @param visitLength
			 */
			private void accumulateVisitLength(long visitLength) {
				if(visitLength>=1&&visitLength<=3) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_1s_3s);
				}else if(visitLength>=4&&visitLength<=6) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_4s_6s);
				}else if(visitLength>=7&&visitLength<=9) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_7s_9s);
				}else if(visitLength>=10&&visitLength<=30) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_10s_30s);
				}else if(visitLength>=31&&visitLength<=60) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_30s_60s);
				}else if(visitLength>=61&&visitLength<=180) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_1m_3m);
				}else if(visitLength>=181&&visitLength<=600) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_3m_10m);
				}else if(visitLength>601&&visitLength<=1800) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_10m_30m);
				}else if(visitLength>1800) {
					sessionAggrSataAccumulator.add(Constants.TIME_PERIOD_30m);
				}
			}
		});
		return filterAggrInfo;
	}
	/**
	 * 计算session各个访问时长和访问步长在总session中的占比并且将计算结果写入mysql中
	 * @param value
	 * @param taskId
	 */
	private static void accumulateAggrSataPersist(String value, long taskId) {
		
		System.out.println(value);
		
		//获取总session数
		long session_count=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		//获取各个时间段以及区间的步长
		long visit_length_1s_3s=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));	
		
		long step_length_1_3=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60=Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));
		
		//计算占比，保留两位小数
		double visit_length_1s_3s_ratio=NumberUtils.formatDouble(
				(double)visit_length_1s_3s/(double)session_count, 2);
		double visit_length_4s_6s_ratio=NumberUtils.formatDouble(
				(double)visit_length_4s_6s/(double)session_count, 2);
		double visit_length_7s_9s_ratio=NumberUtils.formatDouble(
				(double)visit_length_7s_9s/(double)session_count, 2);
		double visit_length_10s_30s_ratio=NumberUtils.formatDouble(
				(double)visit_length_10s_30s/(double)session_count, 2);
		double visit_length_30s_60s_ratio=NumberUtils.formatDouble(
				(double)visit_length_30s_60s/(double)session_count, 2);
		double visit_length_1m_3m_ratio=NumberUtils.formatDouble(
				(double)visit_length_1m_3m/(double)session_count, 2);
		double visit_length_3m_10m_ratio=NumberUtils.formatDouble(
				(double)visit_length_3m_10m/(double)session_count, 2);
		double visit_length_10m_30m_ratio=NumberUtils.formatDouble(
				(double)visit_length_10m_30m/(double)session_count, 2);
		double visit_length_30m_ratio=NumberUtils.formatDouble(
				(double)visit_length_30m/(double)session_count, 2);
		double step_length_1_3_ratio=NumberUtils.formatDouble(
				(double)step_length_1_3/(double)session_count, 2);
		double step_length_4_6_ratio=NumberUtils.formatDouble(
				(double)step_length_4_6/(double)session_count, 2);
		double step_length_7_9_ratio=NumberUtils.formatDouble(
				(double)step_length_7_9/(double)session_count, 2);
		double step_length_10_30_ratio=NumberUtils.formatDouble(
				(double)step_length_10_30/(double)session_count, 2);
		double step_length_30_60_ratio=NumberUtils.formatDouble(
				(double)step_length_30_60/(double)session_count, 2);
		double step_length_60_ratio=NumberUtils.formatDouble(
				(double)step_length_60/(double)session_count, 2);
		
		
		//封装为domain对象并且写入mysql
		SessionAggrSata sessionAggrSata=new SessionAggrSata();
		sessionAggrSata.setTaskId(taskId);
		sessionAggrSata.setSessionCount(session_count);
		sessionAggrSata.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrSata.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrSata.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrSata.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrSata.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrSata.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrSata.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrSata.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrSata.setVisit_length_30m_ratio(visit_length_30m_ratio);
		sessionAggrSata.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrSata.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrSata.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrSata.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrSata.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrSata.setStep_length_60_ratio(step_length_60_ratio);
		//通过DAO工厂获取ISessionAggrSataDAO
		ISessionAggrSataDAO sessionAggrSataDAO=DAOFactory.getSessionAggrSataDAO();
		sessionAggrSataDAO.insert(sessionAggrSata);
	}
	
	
	/**
	 * 重构代码，抽取筛选的session为公共模块
	 */
	
	public static JavaPairRDD<String, Row> getFilterCommonRDD(
			JavaPairRDD<String, String> filterAggrInfo,
			JavaPairRDD<String, Row> sessionIdRowRDD){
		JavaPairRDD<String, Row> session2CategoryRDD=filterAggrInfo
				.join(sessionIdRowRDD)
				.mapToPair(
				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {

						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
		});
		return session2CategoryRDD;
	}
	
	
	
	/**
	 * 获取top10热门品类
	 * 访问过：点击过，下单过，支付过
	 * @param filterAggrInfo
	 * @param sessionIdRowRDD
	 */
	private static List<Tuple2<CategorySortKey, String>> top10Category(
						Long taskId,JavaPairRDD<String, Row> session2DetailRDD) {
		
		/**
		 * 获取符合条件的session
		 */
		//重构代码为公共模块
//		JavaPairRDD<String, Row> session2CategoryRDD=filterAggrInfo
//				.join(sessionIdRowRDD)
//				.mapToPair(
//				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
//
//						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
//					}
//		});
		
		/**
		 * 筛选符合条件的session
		 */
		JavaPairRDD<Long, Long> categoryDetailRDD=session2DetailRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
				
				Row row=tuple._2;
				
				List<Tuple2<Long, Long>> sessionList=new ArrayList<Tuple2<Long,Long>>();
				
				Long clickCategoryIds=row.getLong(6);
				if(clickCategoryIds!=null) {
					sessionList.add(new Tuple2<Long, Long>(clickCategoryIds, 
														   clickCategoryIds));
				}
				
				String orderCategoryIds=row.getString(8);
				if(orderCategoryIds!=null) {
					String[] orderCategoryArr=orderCategoryIds.split(",");
					for(String orderCategoryId:orderCategoryArr) {
						sessionList.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
														       Long.valueOf(orderCategoryId)));
					}
				}
				
				String payCategoryIds=row.getString(10);
				if(payCategoryIds!=null) {
					String[] payCategoryArr=payCategoryIds.split(",");
					for(String payCategoryId:payCategoryArr) {
						sessionList.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
														       Long.valueOf(payCategoryId)));
					}
				}
				
				return sessionList;
			}
		});
		
		
		//必须进行去重，否则取top10的时候会一样
		categoryDetailRDD=categoryDetailRDD.distinct();
		
		/**
		 * 计算点击，下单，支付次数
		 */
		
		//计算点击的次数
		JavaPairRDD<Long, Long> clickCategoryIdCount=cacumulateClickCategoryId(session2DetailRDD);
		
		//计算下单次数
		JavaPairRDD<Long, Long> orderCategoryIdCount=cacumulateOrderCategoryId(session2DetailRDD);
		
		//计算支付次数
		JavaPairRDD<Long, Long> payCategoryIdCount=cacumulatePayCategoryId(session2DetailRDD);
		
		//符合条件的session和点击，下单，支付session进行leftJoin操作
		//不适用join是因为点击，下单，支付操作有的session为空
		//<categoryId,joinInfo>
		JavaPairRDD<Long,String> joinCategoryIdRDD=categoryIdJoin(categoryDetailRDD,clickCategoryIdCount,orderCategoryIdCount,payCategoryIdCount);
		
		/**
		 * 自定义二次排序key
		 */
		/**
		 * 实现二次排序
		 * 映射成<CategorySortKey,joinInfo>的数据格式
		 */
		JavaPairRDD<CategorySortKey, String> joinSesionMapRDD=joinCategoryIdRDD.mapToPair(
				new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
				String joinInfo=tuple._2;
				Long clickCategoryCount=Long.valueOf(StringUtils.getFieldFromConcatString(
							joinInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_COUNT));
				Long orderCategoryCount=Long.valueOf(StringUtils.getFieldFromConcatString(
							joinInfo, "\\|", Constants.FIELD_ORDER_CATEGORY_COUNT));
				Long payCategoryCount=Long.valueOf(StringUtils.getFieldFromConcatString(
							joinInfo, "\\|", Constants.FIELD_PAY_CATEGORY_COUNT));
				
				CategorySortKey categorySortKey=new CategorySortKey(
						clickCategoryCount, orderCategoryCount, payCategoryCount);
				
				return new Tuple2<CategorySortKey, String>(categorySortKey, joinInfo);
			}
		});
		
		//进行降序排序
		joinSesionMapRDD=joinSesionMapRDD.sortByKey(false);
		
		//提取top10条数据
		List<Tuple2<CategorySortKey, String>> top10SortList=joinSesionMapRDD.take(10);
		
		ITop10CategoryDAO categoryDAO=DAOFactory.getTop10CategoryDAO();
		
		for(Tuple2<CategorySortKey, String> tuple:top10SortList) {
			String joinInfo=tuple._2;
			Long categoryId=Long.valueOf(StringUtils.getFieldFromConcatString(
									joinInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			Long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(
									joinInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_COUNT));
			Long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(
									joinInfo, "\\|", Constants.FIELD_ORDER_CATEGORY_COUNT));
			Long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(
									joinInfo, "\\|", Constants.FIELD_PAY_CATEGORY_COUNT));
			
			Top10Category top10Category=new Top10Category();
			top10Category.setTaskId(taskId);
			top10Category.setCategoryId(categoryId);
			top10Category.setClickCount(clickCount);
			top10Category.setOrderCount(orderCount);
			top10Category.setPayCount(payCount);
			//写入mysql数据库
			categoryDAO.insert(top10Category);
		}
		return top10SortList;
	}

	/**
	 * 计算点击量
	 * @param sessionIdRowRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> cacumulateClickCategoryId(
			JavaPairRDD<String, Row> sessionIdRowRDD) {
		/**
		 * 在计算点击量的时候，因为点击量占总数据的一小部分，因此在使用filter算子之后，可能会出现
		 * 每个partition的数据不均匀，因此有的task的执行速度很快，有的很慢，这样很有可能产生数据倾斜
		 * 
		 * 因此 ，我们可以使用压缩操作，压缩partition中的数据，使得数据紧凑，每个task的执行速度平衡
		 * 这时，我们可以在filter算子之后使用coalesce算子
		 */
		JavaPairRDD<String, Row> clickCategoryRDD=sessionIdRowRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Row> tuple) throws Exception {
				return tuple._2.get(6)!=null?true:false;
			}
		}).coalesce(100);
		
		JavaPairRDD<Long, Long> clickCategoryMapRDD=clickCategoryRDD.mapToPair(
				new PairFunction<Tuple2<String,Row>, Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
				Long clickCategoryId=tuple._2.getLong(6);
				return new Tuple2<Long, Long>(clickCategoryId,1L);
			}
			
		});
		
		JavaPairRDD<Long, Long> clickCategoryIdCount=clickCategoryMapRDD.reduceByKey(
				new Function2<Long, Long, Long>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1+v2;
			}
		});
		return clickCategoryIdCount;
	}
	/**
	 * 计算下单量
	 * @param sessionIdRowRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> cacumulateOrderCategoryId(JavaPairRDD<String, Row> sessionIdRowRDD) {
		
		JavaPairRDD<String, Row> orderCategoryIdRDD=sessionIdRowRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Row> tuple) throws Exception {
				Row row=tuple._2();
				return row.getString(8)!=null?true:false;
			}
		});
		
		JavaPairRDD<Long, Long> orderCategoryMapRDD=orderCategoryIdRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
				Row row=tuple._2;
				
				List<Tuple2<Long,Long>> orderList=new ArrayList<Tuple2<Long,Long>>();
				
				String orderCategoryIds=row.getString(8);
				String[] orderCategoryArr=orderCategoryIds.split(",");
				for(String orderCategoryId:orderCategoryArr) {
					orderList.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
				}
				return orderList;
			}
		});
		
		JavaPairRDD<Long, Long> orderCategoryIdsCount=orderCategoryMapRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1+v2;
			}
		});
		
		return orderCategoryIdsCount;
	}
	
	
	/**
	 * 计算支付次数
	 * @param sessionIdRowRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> cacumulatePayCategoryId(JavaPairRDD<String, Row> sessionIdRowRDD) {
		
		JavaPairRDD<String, Row> payCategoryIdsRDD=sessionIdRowRDD.filter(
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Row> tuple) throws Exception {
				Row row = tuple._2;
				return row.getString(10)!=null?true:false;
			}
		});
		
		JavaPairRDD<Long, Long> payCategoryMapRDD=payCategoryIdsRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
				Row row=tuple._2;
				
				List<Tuple2<Long,Long>> payList=new ArrayList<Tuple2<Long,Long>>();
				
				String payCategoryIds=row.getString(10);
				String[] payCategoryArr=payCategoryIds.split(",");
				for(String payCategoryId:payCategoryArr) {
					payList.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
				}
				return payList;
			}
		});
		
		JavaPairRDD<Long, Long> payCategoryIdsCount=payCategoryMapRDD.reduceByKey(
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1+v2;
					}
		});
		
		
		
		return payCategoryIdsCount;
	}

	/**
	 * 进行leftOutJoin操作
	 * @param categoryDetailRDD
	 * @param clickCategoryIdCount
	 * @param orderCategoryIdCount
	 * @param payCategoryIdCount
	 * @return
	 */
	private static JavaPairRDD<Long, String> categoryIdJoin(JavaPairRDD<Long, Long> categoryDetailRDD,
			JavaPairRDD<Long, Long> clickCategoryIdCount, JavaPairRDD<Long, Long> orderCategoryIdCount,
			JavaPairRDD<Long, Long> payCategoryIdCount) {
		
		//join点击量
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD=categoryDetailRDD
								.leftOuterJoin(clickCategoryIdCount);
				
		JavaPairRDD<Long, String> clickJoinRDD=tmpJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) 
																				throws Exception {
				Long categoryId=tuple._1;
				
				Optional<Long> optional=tuple._2._2();
				Long count=0L;
				//如果不为空
				if(optional.isPresent()) {
					count=optional.get();
				}
				String value=Constants.FIELD_CATEGORY_ID+"="+categoryId+"|"+
							 Constants.FIELD_CLICK_CATEGORY_COUNT+"="+count;
				
				return new Tuple2<Long, String>(categoryId, value);
			}
		});
		
		//join下单量
		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpClickJoinRDD=clickJoinRDD
														.leftOuterJoin(orderCategoryIdCount);
		
		JavaPairRDD<Long, String> orderJoinRDD=tmpClickJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
				Long categoryId=tuple._1;
				String value=tuple._2._1;
				Optional<Long> optional=tuple._2._2;
				Long count=0L;
				if(optional.isPresent()) {
					count=optional.get();
				}
				value=value+"|"+Constants.FIELD_ORDER_CATEGORY_COUNT+"="+count;
				return new Tuple2<Long, String>(categoryId, value);
			}
		});
		
		//join支付量
		JavaPairRDD<Long, Tuple2<String, Optional<Long>>> tmpPayJoinRDD=orderJoinRDD
														.leftOuterJoin(payCategoryIdCount);
		
		JavaPairRDD<Long, String> JoinResultRDD=tmpPayJoinRDD.mapToPair(
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>,Long, String>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
																				throws Exception {
				
				Long categoryId=tuple._1;
				Optional<Long> optional=tuple._2._2();
				Long count=0L;
				if(optional.isPresent()) {
					count=optional.get();
				}
				String value=tuple._2._1();
				value=value+"|"+Constants.FIELD_PAY_CATEGORY_COUNT+"="+count;
				return new Tuple2<Long, String>(categoryId, value);
			}
		});
		return JoinResultRDD;
	}
	
	
	/**
	 * 对top10Category生成新的RDD便于后期操作
	 * @param sc	SparkContext
	 * @param top10CatehoryList
	 * @param session2DetailRDD 
	 */
	private static void top10Session(
			final Long taskId,
			JavaSparkContext sc,
			List<Tuple2<CategorySortKey, String>> top10CatehoryList, 
			JavaPairRDD<String,Row> session2DetailRDD) {
		//获取top10品类ID
		List<Tuple2<Long, Long>> top10SessionList=new ArrayList<Tuple2<Long,Long>>();
		String sessioninfo=null;
		for(Tuple2<CategorySortKey, String> tuple:top10CatehoryList) {
			sessioninfo=tuple._2;
			Long categoryId=Long.valueOf(StringUtils.getFieldFromConcatString(
						sessioninfo, "\\|", Constants.FIELD_CATEGORY_ID));
			top10SessionList.add(new Tuple2<Long, Long>(categoryId, categoryId));
		}
		
		//使用sparkContext生成新的PairRDD
		JavaPairRDD<Long, Long> top10CategoryIdRDD=sc.parallelizePairs(top10SessionList);
		
		/**
		 * 获取top10品类被各个session访问的次数
		 */
		
		//首先按照session分组
		JavaPairRDD<String, Iterable<Row>> session2CategoryCountRDD=session2DetailRDD.groupByKey();
		
		JavaPairRDD<Long, String> categoryIdSessionIdCountRDD=session2CategoryCountRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				String sessionId=tuple._1;
				Iterator<Row> it=tuple._2.iterator();
				
				Map<Long,Long> categoryIdCountMap = new HashMap<Long, Long>();
				//计算出该session对每个品类的点击次数
				while(it.hasNext()) {
					Row row=it.next();
					//categoryId不为空
					if(row.get(6)!=null) {
						Long categoryId=row.getLong(6);
						Long count=categoryIdCountMap.get(categoryId);
						if(count==null) {
							count=0L;
						}
						count++;
						categoryIdCountMap.put(categoryId, count);
					}
				}
				//存放各个session对每一个categoryId的点击次数
				List<Tuple2<Long,String>> categoryIdSessionCountList=new ArrayList<Tuple2<Long,String>>();
				
				for(Map.Entry<Long, Long> map:categoryIdCountMap.entrySet()) {
					Long categoryId=map.getKey();
					Long count=map.getValue();
					String value=sessionId+","+count;
					categoryIdSessionCountList.add(new Tuple2<Long, String>(categoryId, value));
				}
				
				return categoryIdSessionCountList;
			}
		});
		
		//top10品类ID与session点击categoryId进行join操作
		//获取top10品类被每个session点击的次数
		JavaPairRDD<Long, String> categorySessionJoinRDD=top10CategoryIdRDD
				.join(categoryIdSessionIdCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> t) throws Exception {
						Long categoryId=t._1;
						String value=t._2._2;
						return new Tuple2<Long, String>(categoryId, value);
					}
				});
		
		/**
		 * 自定义算法实现各个品类top10活跃用户
		 */
		
		//首先按照品类分组
		JavaPairRDD<Long, Iterable<String>> categorySessionRDD=categorySessionJoinRDD.groupByKey();
		
		//获取top10活跃用户
		JavaPairRDD<String, String> top10SessionRDD=categorySessionRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple)
														throws Exception {
						Long  categoryId=tuple._1;
						Iterator<String> it=tuple._2.iterator();
						//存放top10活跃用户
						String[] top10Session=new String[10];
						
						while(it.hasNext()) {
							String value=it.next();
							String sessionId=value.split(",")[0];
							Long count=Long.valueOf(value.split(",")[1]);
							for(int i=0;i<top10Session.length;i++) {
								//如果top10数组中当前位置没有数据，则将当前session放入数组中
								if(top10Session[i]==null) {
									top10Session[i]=value;
									break;
								}else {
									Long _count=Long.valueOf(top10Session[i].split(",")[1]);
									//如果当前session的点击次数大于top10该位置上session的点击次数，则将该位置上的
									//session向后移动一位，将当前session插入数组
									if(count>_count) {
										for(int j=9;j>i;j--) {
											top10Session[j]=top10Session[j-1];
										}
										top10Session[i]=value;
										break;
									}
								}
							}
						}
						
						/**
						 * 将top10活跃用户插入数据库中，并且放入list中返回
						 */
						
						List<Tuple2<String, String>> list=new ArrayList<Tuple2<String,String>>();
						
						ITop10CategorySessionDAO top10CategorySessionDAO=DAOFactory.getTop10CategorySessionDAO();
						
						for(String str:top10Session) {
							if(str!=null) {
								String sessionId=str.split(",")[0];
								Long clickCount=Long.valueOf(str.split(",")[1]);
								
								Top10CategorySession top10CategorySession=new Top10CategorySession();
								top10CategorySession.setTaskId(taskId);
								top10CategorySession.setCategoryId(categoryId);
								top10CategorySession.setSessionId(sessionId);
								top10CategorySession.setClickCount(clickCount);
								//写入数据库
								top10CategorySessionDAO.insert(top10CategorySession);
								//插入list中
								list.add(new Tuple2<String, String>(sessionId, sessionId));
							}
						}
						return list;
					}
		});
		
		
		/**
		 * 获取top10活跃用于的session明细数据写入数据库
		 */
		/**
		 * 进行join操作，并且将session明细数据放入数据库中
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionIdAndDetailRDD=top10SessionRDD.join(session2DetailRDD);
		
		sessionIdAndDetailRDD.foreach(
				new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				String sessionId=tuple._1;
				Row row=tuple._2._2;
				SessionDetail sessionDetail=new SessionDetail();
				sessionDetail.setTaskId(taskId);
				sessionDetail.setUserId(row.getLong(1));
				sessionDetail.setSessionId(row.getString(2));
				sessionDetail.setPageId(row.getLong(3));
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));
				sessionDetail.setClickCategoryId(row.getLong(6));
				sessionDetail.setClickProductId(row.getLong(7));
				sessionDetail.setOrderCategoryIds(row.getString(8));
				sessionDetail.setOrderProductIds(row.getString(9));
				sessionDetail.setPayCategoryIds(row.getString(10));
				sessionDetail.setPayProductIds(row.getString(11));
				//从DAO工厂获取DAO接口
				ISessionDetailDAO sessionDetailDAOImpl=DAOFactory.getSessionDetailDAO();
				//插入数据库
				sessionDetailDAOImpl.insert(sessionDetail);
			}
		});
	}
}
