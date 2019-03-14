package cn.just.spark.constant;
/**
 * JDBC常量接口
 * @author shinelon
 *
 */
public interface Constants {
	/**
	 * 项目配置常量
	 */
	public String JDBC_DRIVER="jdbc.driver";
	public String JDBC_DATASOURCE_SIZE="jdbc.datasource.size";
	
	//本地测试环境mysql数据库
	public String JDBC_URL="jdbc.url";
	public String JDBC_USER="jdbc.user";
	public String JDBC_PASSWORD="jdbc.password";
	
	//生产环境测试mysql数据库
	public String JDBC_URL_PROD="jdbc.url.prod";
	public String JDBC_USER_PROD="jdbc.user.prod";
	public String JDBC_PASSWORD_PROD="jdbc.password.prod";
	
	
	public String SPARK_LOCAL="spark.local";
	
	public String SPARK_LOCAL_TASKID_SESSION="spark.local.taskid.session";
	public String SPARK_LOCAL_TASKID_PAGE="spark.local.taskid.page";
	
	public String SPARK_LOCAL_TASKID_PRODUCT="spark.local.taskid.product";
	
	/**
	 * kafka相关参数
	 */
	public String KAFKA_METADATA_BROKER_LIST="kafka.metadata.broker.list";
	
	public String KAFKA_TOPICS="kafka.topics";
	
	
	/**
	 * 作业相关常量
	 */
	public String FIELD_START_TIME="startTime";
	public String FIELD_END_TIME="endTime";
	public String FIELD_SESSION_ID="sessionId";
	public String FIELD_SEARCH_KEY_WORD="searchKeyWord";
	public String FIELD_CLICK_CATEGORY_ID="clickCategoryId";
	public String FIELD_AGE="age";
	public String FIELD_PROFESSIONAL="professional";
	public String FIELD_CITY="city";
	public String FIELD_SEX="sex";
	public String FIELD_VISIT_LENGTH="visitLength";
	public String FIELD_STEP_LENGTH="stepLength";
	public String FIELD_CATEGORY_ID="categoryId";
	public String FIELD_CLICK_CATEGORY_COUNT="clickCategoryCount";
	public String FIELD_ORDER_CATEGORY_COUNT="orderCategoryCount";
	public String FIELD_PAY_CATEGORY_COUNT="payCategoryCount";
	
	/**
	 * 任务相关常量
	 */
	public String TASK_START_DATE="startDate";
	public String TASK_END_DATE="endDate";
	public String TASK_START_AGE="startAge";
	public String TASK_END_AGE="endAge";
	public String  TASK_PROFESSIONALS="professional";
	public String TASK_CITY="city";
	public String TASK_SEX="sex";
	public String TASK_KEY_WORD="keyWords";
	public String TASK_CLICK_CATEGORY_ID="clickCategoryId";
	
	
	
	/**
	 * Spark相关常量
	 */
	public String SPARK_APP_NAME_SESSION="spark.app.name.session";
	
	public String SPARK_APP_NAME_PAGE="spark.app.name.page";
	
	public String SESSION_COUNT = "session_count";
	
	public String TIME_PERIOD_1s_3s = "1s_3s";
	public String TIME_PERIOD_4s_6s = "4s_6s";
	public String TIME_PERIOD_7s_9s = "7s_9s";
	public String TIME_PERIOD_10s_30s = "10s_30s";
	public String TIME_PERIOD_30s_60s = "30s_60s";
	public String TIME_PERIOD_1m_3m = "1m_3m";
	public String TIME_PERIOD_3m_10m = "3m_10m";
	public String TIME_PERIOD_10m_30m = "10m_30m";
	public String TIME_PERIOD_30m = "30m";
	
	public String STEP_PERIOD_1_3 = "1_3";
	public String STEP_PERIOD_4_6 = "4_6";
	public String STEP_PERIOD_7_9 = "7_9";
	public String STEP_PERIOD_10_30 = "10_30";
	public String STEP_PERIOD_30_60 = "30_60";
	public String STEP_PERIOD_60 = "60";
	public String PARAM_END_DATE = null;
	
	//参数相关常量
	public String PARAM_TARGET_PAGE_FLOW="targetPageFlow";
	
	
}
