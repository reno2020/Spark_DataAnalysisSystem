package cn.just.spark.domain;
/**
 * 各区域top3热门商品
 * @author shinelon
 *
 */
public class AreaTop3Product {
	
	/**
	 * task_id        | int(11)      | YES  |     | NULL    |       |
		| area           | varchar(255) | YES  |     | NULL    |       |
		| area_level     | varchar(255) | YES  |     | NULL    |       |
		| product_id     | int(11)      | YES  |     | NULL    |       |
		| city_infos     | varchar(255) | YES  |     | NULL    |       |
		| click_count    | int(11)      | YES  |     | NULL    |       |
		| product_name   | varchar(255) | YES  |     | NULL    |       |
		| product_status | varchar(255) | YES  |     | NULL
	 */
	
	private Long taskId;
	private String area;
	private String areaLevel;
	private Long productId;
	private String cityInfos;
	private Long clickCount;
	private String productName;
	private String productStatus;
	public Long getTaskId() {
		return taskId;
	}
	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}
	public String getArea() {
		return area;
	}
	public void setArea(String area) {
		this.area = area;
	}
	public String getAreaLevel() {
		return areaLevel;
	}
	public void setAreaLevel(String areaLevel) {
		this.areaLevel = areaLevel;
	}
	public Long getProductId() {
		return productId;
	}
	public void setProductId(Long productId) {
		this.productId = productId;
	}
	public String getCityInfos() {
		return cityInfos;
	}
	public void setCityInfos(String cityInfos) {
		this.cityInfos = cityInfos;
	}
	public Long getClickCount() {
		return clickCount;
	}
	public void setClickCount(Long clickCount) {
		this.clickCount = clickCount;
	}
	public String getProductName() {
		return productName;
	}
	public void setProductName(String productName) {
		this.productName = productName;
	}
	public String getProductStatus() {
		return productStatus;
	}
	public void setProductStatus(String productStatus) {
		this.productStatus = productStatus;
	}
	
	
	

}
