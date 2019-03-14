package cn.just.spark.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.alibaba.fastjson.JSONObject;

import cn.just.spark.constant.Constants;
import cn.just.spark.dao.IPageSplitConvertRateDAO;
import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.dao.impl.DAOFactory;
import cn.just.spark.dao.impl.PageSplitConvertRateDAOImpl;
import cn.just.spark.domain.PageSplitConvertRate;
import cn.just.spark.domain.Task;
import cn.just.spark.utils.DateUtils;
import cn.just.spark.utils.NumberUtils;
import cn.just.spark.utils.ParamUtils;
import cn.just.spark.utils.SparkUtils;
import scala.Tuple2;

/**
 * 项目第二个模块开发：
 * 页面单挑转换率Spark程序开发
 * @author shinelon
 *
 */
public class PageOneStepConvertRateSpark {
	
	public static void main(String[] args) {
		
		//1.创建Spark上下文
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_PAGE);
		
		SparkUtils.setMaster(conf);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//获取SQLContext
		SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());
		
		//2.模拟数据
		SparkUtils.mockData(sc, sqlContext);
		
		//3.查询任务，获取任务参数
		Long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
		
		//获取Task DAO类
		ITaskDAO taskDao = DAOFactory.getTaskDAO();
		Task task = taskDao.findById(taskId);
		if(task==null) {
			System.out.println(new Date()+" : can not find task with id: ["+taskId+"]");
		}
		
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		//4.获取指定范围内用户访问行为数据
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		
		//获取session粒度的用户访问数据
		//如果是用户A访问了页面3，用户B访问页面4，则不能计算为切片3_4，因此必须是session粒度的数据
		//转换为<sessionId,Row>的格式
		JavaPairRDD<String,Row> session2ActionRDD = session2actionRDD(actionRDD);
		JavaPairRDD<String,Iterable<Row>> aggrSession2actionRDD = session2ActionRDD.groupByKey();
		
		//页面生成以及页面匹配算法
		JavaPairRDD<String,Integer> splitCountRDD = generatePageAndMatchFlowRDD(sc, taskParam, aggrSession2actionRDD);
		Map<String,Object> splitCountMap = splitCountRDD.countByKey();
		
		//计算起始页面的PV
		Long startPagePV = getStartPagePV(taskParam, aggrSession2actionRDD);
		
		System.out.println("起始页面PV为："+startPagePV);
		//计算页面切片转换率
		Map<String,Double> convertRateMap = computePageSplitConvertRate(taskParam, splitCountMap, startPagePV);
		
		//持久化数据到MYSQL数据库中
		persistConvertRate(taskId, convertRateMap);
	}
	/**
	 * 获取session粒度的用户行为访问数据
	 * @param actionRDD
	 */
	private static JavaPairRDD<String,Row> session2actionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(new PairFunction<Row,String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String sessionId=row.getString(2);
				return new Tuple2<String,Row>(sessionId,row);
			}
		});
	}
	/**
	 * 页面切片生成以及页面流匹配算法
	 * @param sc
	 * @param taskParam
	 * @param aggrSession2actionRDD
	 * @return
	 */
	public static JavaPairRDD<String,Integer> generatePageAndMatchFlowRDD(
			JavaSparkContext sc,
			JSONObject taskParam,
			JavaPairRDD<String,Iterable<Row>> aggrSession2actionRDD){
		//获取task中的页面流任务
		String pageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		final Broadcast<String> pageFlowBroadCast = sc.broadcast(pageFlow);
		
		return aggrSession2actionRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Row>>,String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				//存放返回值的list
				List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
				
				//获取用户访问信息
				Iterator<Row> iterator = tuple._2.iterator();
				//将用户session数据放入list中
				List<Row> rows = new ArrayList<Row>();
				while(iterator.hasNext()) {
					rows.add(iterator.next());
				}
				//按照页面的访问时间排序
				Collections.sort(rows, new Comparator<Row>() {

					@Override
					public int compare(Row o1, Row o2) {
						String time1 = o1.getString(4);
						String time2 = o2.getString(4);
						Date date1 = DateUtils.parseDateKey(time1);
						Date date2 = DateUtils.parseDateKey(time2);
						return (int) (date1.getTime()-date2.getTime());
					}
				});
				//从参数中获取用户指定的页面流
				//页面形如 1,5,6,3,4
				String[] targetPages = pageFlowBroadCast.value().split(",");
				
				/**
				 * 页面切片生成以及页面流匹配算法
				 */
				
				Long lastPageId=null;
				
				for(Row row:rows) {
					long pageId=row.getLong(3);
					//第一个页面
					if(lastPageId==null) {
						lastPageId=pageId;
						continue;
					}
					
					//非第一个页面生成切片
					String pageSplit = lastPageId+"_"+pageId;
					
					//页面流匹配算法
					for(int i=1;i<targetPages.length;i++) {
						//比如用户指定的页面流是：
						//3,2,4,8,6,9
						//则从下标为1开始遍历，匹配切片，比如第一个切片就是3_2
						String split = targetPages[i-1]+"_"+targetPages[i];
						if(pageSplit.equals(split)) {
							list.add(new Tuple2<String,Integer>(pageSplit,1));
							break;
						}
					}
					lastPageId=pageId;
				}
				return list;
			}
		});
	}
	
	/**
	 * 计算页面流起始页面的PV
	 * @param taskParam
	 * @param sessionid2actionsRDD
	 * @return
	 */
	public static Long getStartPagePV(
			JSONObject taskParam,
			JavaPairRDD<String, Iterable<Row>> aggrSession2actionRDD) {
		String pageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		//获取起始页面ID
		final Long startPageId = Long.valueOf(pageFlow.split(",")[0]);
		
		JavaRDD<Long> startPageRDD = aggrSession2actionRDD.flatMap(
				new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				List<Long> list = new ArrayList<Long>();
				Iterator<Row> iterator = tuple._2.iterator();
				while(iterator.hasNext()) {
					Row row = iterator.next();
					long pageId = row.getLong(3);
					if(pageId == startPageId) {
						list.add(pageId);
					}
				}
				return list;
			}
		});
		return startPageRDD.count();
	}
	/**
	 * 计算页面切片转换率
	 * @param taskParam
	 * @param splitCountMap
	 * @param startPagePV
	 * @return
	 */
	private static Map<String,Double> computePageSplitConvertRate(
			JSONObject taskParam,
			Map<String,Object> splitCountMap,
			Long startPagePV){
		Map<String,Double> convertRateMap = new HashMap<String,Double>();
		String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
		String[] pages = targetPageFlow.split(",");
		/**
		 * 计算目标切片转换率
		 * 比如页面流：5,6,3,2,1,4
		 * 则5_6切片转换率= 5pv / 5_6pv
		 * 6_3切片转换率=5_6pv / 6_3pv
		 */
		Long lastPageSplitPV = 0L;
		for(int i=1;i<pages.length;i++) {
			String targetPageSplit = pages[i-1]+"_"+pages[i];
			Long targetPageSplitPV = Long.valueOf(String.valueOf(splitCountMap.get(targetPageSplit)));
			Double pageSplitConvertRate = 0.0;
			if(i==1) {
				pageSplitConvertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPV / (double)startPagePV, 2);
			}else{
				pageSplitConvertRate = NumberUtils.formatDouble(
						(double)targetPageSplitPV / (double)lastPageSplitPV, 2);
			}
			convertRateMap.put(targetPageSplit, pageSplitConvertRate);
			lastPageSplitPV = targetPageSplitPV;
		}
		return convertRateMap;
	}
	/**
	 * 持久化数据到mysql数据库中
	 * @param taskId
	 * @param pvMap
	 */
	private static void persistConvertRate(
			Long taskId,
			Map<String,Double> convertRateMap) {
		StringBuffer buffer = new StringBuffer("");
		//数据库中convertRate字段拼接成 pageSplit=convertRate|pageSplit=convertRate的格式
		for(Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
			String pageSplit = entry.getKey();
			Double convertRate = entry.getValue();
//			System.out.println("MAP:"+pageSplit+"=="+convertRate);
			buffer.append(pageSplit+"="+convertRate+"|");
		}
		String convertRateBuffer = buffer.toString();
//		System.out.println("字符串为:"+convertRateBuffer);
		convertRateBuffer = convertRateBuffer.substring(0 , convertRateBuffer.length()-1);
		PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
		pageSplitConvertRate.setTaskId(taskId);
		pageSplitConvertRate.setConvertRate(convertRateBuffer);
		//从DAO工厂中获取DAO接口
		IPageSplitConvertRateDAO convertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
		convertRateDAO.insert(pageSplitConvertRate);
	}
}
