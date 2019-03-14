package cn.just.spark.domain;
/**
 * 存放随机抽取session的表的实体类
 * 2018-08-05
 * @author shinelon
 *
 */
public class SessionRandomExtract {
	private Long taskId;
	private String sessionId;
	private String startTime;
	private String searchKeywords;
	private String clickCategoryIds;
	public Long getTaskId() {
		return taskId;
	}
	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getSearchKeywords() {
		return searchKeywords;
	}
	public void setSearchKeywords(String searchKeywords) {
		this.searchKeywords = searchKeywords;
	}
	public String getClickCategoryIds() {
		return clickCategoryIds;
	}
	public void setClickCategoryIds(String clickCategoryIds) {
		this.clickCategoryIds = clickCategoryIds;
	}
	
	
}
