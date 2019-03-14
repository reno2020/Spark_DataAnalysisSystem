package cn.just.spark.domain;
/**
 * top10活跃用户表实体类
 * @author shinelon
 *
 */
public class Top10CategorySession {

	private Long taskId;
	private Long categoryId;
	private String sessionId;
	private Long clickCount;
	public Long getTaskId() {
		return taskId;
	}
	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}
	public Long getCategoryId() {
		return categoryId;
	}
	public void setCategoryId(Long categoryId) {
		this.categoryId = categoryId;
	}
	public String getSessionId() {
		return sessionId;
	}
	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}
	public Long getClickCount() {
		return clickCount;
	}
	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}
	
}
