package cn.just.spark.ad;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import cn.just.spark.conf.ConfigurationManager;
import cn.just.spark.constant.Constants;
import cn.just.spark.dao.IAdBlockListDAO;
import cn.just.spark.dao.IAdStatDAO;
import cn.just.spark.dao.IAdUserClickCountDAO;
import cn.just.spark.dao.impl.DAOFactory;
import cn.just.spark.domain.AdBlockList;
import cn.just.spark.domain.AdStat;
import cn.just.spark.domain.AdUserClickCount;
import cn.just.spark.utils.DateUtils;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 广告流量点击实时统计Spark作业
 * 2018-11-19
 * @author shinelon
 *
 */
public class AdClickRealTimeStateSpark {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("AdClickRealTimeStateSpark");
		
		//构建Spark Streaming上下文
		//统计每5秒batch内的数据
		JavaStreamingContext  jsc = new JavaStreamingContext(conf,Durations.seconds(5));
		
		//kafka broker list
		HashMap<String,String> brokerListMap = new HashMap<String,String>();
		brokerListMap.put(Constants.KAFKA_METADATA_BROKER_LIST, 
						ConfigurationManager.getProperties(Constants.KAFKA_METADATA_BROKER_LIST));
		
		String topics = ConfigurationManager.getProperties(Constants.KAFKA_TOPICS);
		//多个topic之间使用逗号分隔
		String[] topicsList = topics.split(",");		
		
		//topic set
		HashSet<String> topicsSet = new HashSet<String>();
		for(String topic:topicsList) {
			topicsSet.add(topic);
		}
		
		//使用kafka direct api
		//第一个参数是Spark Streaming上下文，第二个参数没有实际意义，第三个参数就是从消息队列中读取的数据
		//第四个和第五个参数对应第二个和第三个参数的类型的解码类型
		//第六个和第七个参数分别是Map类型对应brokerList列表和Set类型对应topic的列表
		JavaPairInputDStream<String,String> dailyAdClickReamTime = KafkaUtils.createDirectStream(jsc, 
									String.class, String.class, 
									StringDecoder.class, StringDecoder.class, 
									brokerListMap, topicsSet);
		
		
		
		//根据动态黑名单从源头过滤黑名单用户
		JavaPairDStream<String,String> filteredAdClickRealTime = filterByBlockList(dailyAdClickReamTime);
		
		//生成动态黑名单用户列表
		generationDynamicBlockList(filteredAdClickRealTime);
		
		//业务一：实时统计每天每个省份的各个城市的各个广告的点击次数，保存一份到集群的内存中，并且持久化到数据库中
		JavaPairDStream<String,Long> aggreateAdRealTimeDStream = adRealTimeClickCountStatAndPersist(filteredAdClickRealTime);
		
		
		//启动Spark Streaming，并且等待其执行结束并且关闭上下文
		jsc.start();
		jsc.awaitTermination();
		jsc.close();
		
	}
	
	/**
	 * 统计每天每个省份的各个城市的各个广告的点击次数并且持久化到数据库，在集群的内存中也需要保存一份数据
	 * @param filteredAdClickRealTime
	 * @return
	 */
	private static JavaPairDStream<String, Long> adRealTimeClickCountStatAndPersist(
			JavaPairDStream<String, String> filteredAdClickRealTime) {
		//将数据转换为<date_province_city_adId,1>的格式
				JavaPairDStream<String,Long> mapDStream = filteredAdClickRealTime.mapToPair(
						new PairFunction<Tuple2<String,String>, String, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						String[] splitedMsg = tuple._2.split(" ");
						String timestamp = splitedMsg[0];
						Date date = new Date(timestamp);
						String timeKey = DateUtils.formatDateKey(date);
						String privince = splitedMsg[1];
						String city = splitedMsg[2];
						Long adId = Long.valueOf(splitedMsg[4]);
						String key = timeKey+"_"+privince+"_"+city+"_"+adId;
						return new Tuple2<String,Long>(key,1L);
					}
				});
				
				/**
				 * UpdateStateByKey的主要功能:
				 * 1、为Spark Streaming中每一个Key维护一份state状态，
				 * 	  state类型可以是任意类型的， 可以是一个自定义的对象，那么更新函数也可以是自定义的。
				 * 2、通过更新函数对该key的状态不断更新，对于每个新的batch而言，Spark Streaming会在使用
				 *    updateStateByKey的时候为已经存在的key进行state的状态更新
				 *    
				 *    为了防止数据丢失，可以开启checkpoint功能
				 */
				JavaPairDStream<String,Long> aggreateAdRealTimeDStream = mapDStream.updateStateByKey(
						new Function2<List<Long>, Optional<Long>, Optional<Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
						//对每一个key判断是否存在当前状态，并且进行累加操作
						Long clickCount = 0L;
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						for(Long value:values) {
							clickCount += value;
						}
						return optional.of(clickCount);
					}
				});
				
				//持久化一份到数据库中
				aggreateAdRealTimeDStream.foreachRDD(new Function<JavaPairRDD<String,Long>, Void>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
						
						rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>() {

							private static final long serialVersionUID = 1L;

							@Override
							public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
								List<AdStat> adStatList = new ArrayList<AdStat>();
								while(iterator.hasNext()) {
									Tuple2<String,Long> tuple = iterator.next();
									String[] msgSplited = tuple._1.split(" ");
									String date = msgSplited[0];
									String province = msgSplited[1];
									String city = msgSplited[2];
									Long adId = Long.valueOf(msgSplited[3]);
									Long clickCount = tuple._2;
									AdStat adStat = new AdStat();
									adStat.setDate(date);
									adStat.setProvince(province);
									adStat.setCity(city);
									adStat.setAdId(adId);
									adStat.setClickCount(clickCount);
									adStatList.add(adStat);
								}
								//批量插入到数据库中
								IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
								adStatDAO.updateBatch(adStatList);
							}
						});
						return null;
					}
				});
				return aggreateAdRealTimeDStream;
	}

	/**
	 * 生成动态黑名单列表
	 * @param filteredAdClickRealTime
	 */
	private static void generationDynamicBlockList(JavaPairDStream<String, String> filteredAdClickRealTime) {
		//从消息队列中读取到的消息的格式是：timestamp province city userid adid
				//使用mapToPair算子需要转换为<timeKey_userid_adid,1>的格式，其中timeKey是将时间戳转换为yyyyMMdd的格式
				JavaPairDStream<String,Long> dayUserAdClickRDD = filteredAdClickRealTime.mapToPair(
						new PairFunction<Tuple2<String,String>, String, Long>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						//第二个参数才是从消息队列中获取的数据
						String brokerMsg = tuple._2;
						//数据之间是用空格隔开的
						String[] msgRecorder = brokerMsg.split(" ");  
						String timestamp = msgRecorder[0];
						Date date = new Date(timestamp);
						String timeKey = DateUtils.formatDateKey(date);
						Long userId = Long.valueOf(msgRecorder[3]);
						Long adId = Long.valueOf(msgRecorder[4]);
						String key = timeKey+"_"+userId+"_"+adId;
						return new Tuple2<String,Long>(key,1L);
					}
				});
				
				//获取了每个batch内每天每个用户对每个广告的点击次数
				JavaPairDStream<String,Long> dailyUserAdClickCountRDD = dayUserAdClickRDD.reduceByKey(
						new Function2<Long, Long, Long>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1+v2;
					}
				});
				
				//将每天每个用户对指定广告点击的次数更新到数据库中持久化，使用批量插入的方式
				dailyUserAdClickCountRDD.foreachRDD(
						new Function<JavaPairRDD<String,Long>, Void>() {
					
					private static final long serialVersionUID = 1L;

					@Override
					public Void call(JavaPairRDD<String, Long> javaPairRDD) throws Exception {
						javaPairRDD.foreachPartition(
								new VoidFunction<Iterator<Tuple2<String,Long>>>() {

							private static final long serialVersionUID = 1L;

							@Override
							public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
								// 对每个分区的数据就去获取一次连接对象
								// 每次都是从连接池中获取，而不是每次都创建
								IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
								List<AdUserClickCount> adUserClickCountList = new ArrayList<AdUserClickCount>();
								while(iterator.hasNext()) {				//相当于scala中的foreach操作
									Tuple2<String,Long> tuple = iterator.next();
									String key = tuple._1;
									Long count = tuple._2;
									String[] keyArr = key.split("_");
									AdUserClickCount adUserClickCount = new AdUserClickCount();
									//将yyyyMMdd日期格式转换为yyyy-MM-dd的格式
									String date = DateUtils.formatDate(DateUtils.parseDateKey(keyArr[0]));
									Long userId = Long.valueOf(keyArr[1]);
									Long adId = Long.valueOf(keyArr[2]);
									adUserClickCount.setDate(date);
									adUserClickCount.setUserId(userId);
									adUserClickCount.setClickCount(count);
									adUserClickCountList.add(adUserClickCount);
								}
								adUserClickCountDAO.updateBatch(adUserClickCountList);
							}
						});
						return null;
					}
				});
				
				//如果每个用户点击广告的次数超过了100次，则将其实时加入黑名单中
				//使用聚合后的数据进行过滤，可以减少计算的数据量
				JavaPairDStream<String,Long> blackListDStream = dailyUserAdClickCountRDD.filter(
						
						new Function<Tuple2<String,Long>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Long> tuple) throws Exception {
						String key = tuple._1;
						String[] keySplits = key.split("_");
						String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplits[0]));
						Long userId = Long.valueOf(keySplits[1]);
						Long adId = Long.valueOf(keySplits[2]);
						IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
						Long clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId, adId);
						//如果用户广告点击次数大于等于100则加入黑名单列表
						if(clickCount >= 100) {
							return true;
						}
						return false;
					}
				});
				
				//对于过滤出来的黑名单用户有可能会出现userId重复的情况，
				//比如20181120_1001_01 1001用户对01广告的点击次数超过了100
				//20181120_1001_02  1001用户对02广告的点击次数也超过了100
				//上面实际上是同一个用户，如果不对UserId去重则会在黑名单中重复出现
				
				JavaDStream<Long> blockUserIdListDStream = blackListDStream.map(
						new Function<Tuple2<String,Long>, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Tuple2<String, Long> tuple) throws Exception {
						String key = tuple._1;
						String[] keySplits = key.split("_");
						Long userId = Long.valueOf(keySplits[1]);
						return userId;
					}
				});
				
				//transform操作将一个RDD转换为另一个RDD
				//对userId进行去重操作
				JavaDStream<Long> distinctBlockListDStream = blockUserIdListDStream.transform(
						new Function<JavaRDD<Long>, JavaRDD<Long>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
						return rdd.distinct();
					}
				});
				
				/**
				 * 对每一个RDD的每一个分区的数据持久化到数据库中
				 */
				distinctBlockListDStream.foreachRDD(
						new Function<JavaRDD<Long>, Void>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Void call(JavaRDD<Long> javaRDD) throws Exception {
						javaRDD.foreachPartition(
								new VoidFunction<Iterator<Long>>() {

							private static final long serialVersionUID = 1L;

							@Override
							public void call(Iterator<Long> iterator) throws Exception {
								List<AdBlockList> blockList = new ArrayList<AdBlockList>();
								while(iterator.hasNext()) {
									Long userId = iterator.next();
									AdBlockList adBlockList = new AdBlockList();
									blockList.add(adBlockList);
								}
								IAdBlockListDAO adBlockListDAO = DAOFactory.getAdBlockListDAO();
								//批量插入黑名单列表中
								adBlockListDAO.insertBatch(blockList);
								//在动态生成黑名单之后就可以在每次数据的源头进行黑名单的过滤操作，对不符合条件的用户直接进行过滤操作
							}
						});
						return null;
					}
				});
	}

	/**
	 * 根据黑名单过滤用户
	 * @param dailyAdClickReamTime
	 * @return
	 */
	private static JavaPairDStream<String,String> filterByBlockList(
			JavaPairInputDStream<String, String> dailyAdClickReamTime) {

		JavaPairDStream<String, String> filteredAdClickRealTime = dailyAdClickReamTime.transformToPair(
				new Function<JavaPairRDD<String,String>, JavaPairRDD<String,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
				
				IAdBlockListDAO adBlcokListDAO = DAOFactory.getAdBlockListDAO();
				List<AdBlockList> blockList = adBlcokListDAO.findAll();
				List<Tuple2<Long,Boolean>> tupleList = new ArrayList<Tuple2<Long,Boolean>>();
				//将黑名单用户转换为<userId,true>的格式
				for(AdBlockList adBlockList :blockList) {
					tupleList.add(new Tuple2<Long,Boolean>(adBlockList.getUserId(),true));
				}
				
				JavaSparkContext sc =new JavaSparkContext(rdd.context());
				JavaPairRDD<Long,Boolean> blockRDD = sc.parallelizePairs(tupleList);
				
				JavaPairRDD<Long, Tuple2> mapRDD = rdd.mapToPair(
						new PairFunction<Tuple2<String,String>, Long, Tuple2>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Tuple2> call(Tuple2<String, String> tuple) throws Exception {
						String msg = tuple._2;
						String[] splitMsg = msg.split(" ");
						Long userId = Long.valueOf(splitMsg[3]);
						return new Tuple2<Long,Tuple2>(userId,tuple);
					}
				});
				
				//使用左外连接join黑名单用户
				JavaPairRDD<Long, Tuple2<Tuple2, Optional<Boolean>>> joinRDD = mapRDD.leftOuterJoin(blockRDD);
				
				JavaPairRDD<Long, Tuple2<Tuple2, Optional<Boolean>>> filteredPairRDD = joinRDD.filter(
						new Function<Tuple2<Long,Tuple2<Tuple2,Optional<Boolean>>>, Boolean>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<Long, Tuple2<Tuple2, Optional<Boolean>>> tuple) throws Exception {
						Optional<Boolean> optional = tuple._2._2();
						//如果值不为空，且为ture，即黑名单用户，数据过滤
						if(optional.isPresent()&&optional.get()) {
							return false;
						}
						return true;
					}
				});
				
				JavaPairRDD<String,String> mapFilterRDD = filteredPairRDD.mapToPair(
						new PairFunction<Tuple2<Long,Tuple2<Tuple2,Optional<Boolean>>>, String, String>() {

							private static final long serialVersionUID = 1L;
		
							@Override
							public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2, Optional<Boolean>>> tuple)
									throws Exception {
								return tuple._2._1;
							}
						});
						return mapFilterRDD;
			}
		});
		return filteredAdClickRealTime;
	}

}
