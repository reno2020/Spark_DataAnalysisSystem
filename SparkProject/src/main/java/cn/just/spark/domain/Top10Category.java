package cn.just.spark.domain;
/**
 * top10热门品类表实体类
 * 2018-08-06
 * @author shinelon
 *
 */
public class Top10Category {
	private Long taskId;
	private Long categoryId;
	private Long clickCount;
	private Long orderCount;
	private Long payCount;
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
	public Long getClickCount() {
		return clickCount;
	}
	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}
	public Long getOrderCount() {
		return orderCount;
	}
	public void setOrderCount(Long orderCount) {
		this.orderCount = orderCount;
	}
	public Long getPayCount() {
		return payCount;
	}
	public void setPayCount(Long payCount) {
		this.payCount = payCount;
	}
	

}
