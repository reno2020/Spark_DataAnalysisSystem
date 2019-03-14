package cn.just.spark.session;

import java.io.Serializable;

import scala.math.Ordered;

/**
 * 自定义二次排序key（必须实现序列化接口）
 * 
 * 在该类中封装需要进行二次排序的多个字段
 * 
 * 2018-08-06
 * @author shinelon
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>,Serializable{
	
	private long clickCategoryCount;
	private long orderCategoryCount;
	private long payCategoryCount;

	public CategorySortKey(Long clickCategoryCount, Long orderCategoryCount, Long payCategoryCount) {
		super();
		this.clickCategoryCount = clickCategoryCount;
		this.orderCategoryCount = orderCategoryCount;
		this.payCategoryCount = payCategoryCount;
	}

	@Override
	public boolean $greater(CategorySortKey other) {
		if(clickCategoryCount>other.clickCategoryCount) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount>other.orderCategoryCount) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount==other.orderCategoryCount&&
				payCategoryCount>other.payCategoryCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $greater$eq(CategorySortKey other) {
		if($greater(other)) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount==other.orderCategoryCount&&
				payCategoryCount==other.payCategoryCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(CategorySortKey other) {
		if(clickCategoryCount<other.clickCategoryCount) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount<other.orderCategoryCount) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount==other.orderCategoryCount&&
				payCategoryCount<other.payCategoryCount) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortKey other) {
		if($less(other)) {
			return true;
		}else if(clickCategoryCount==other.clickCategoryCount&&
				orderCategoryCount==other.orderCategoryCount&&
				payCategoryCount==other.payCategoryCount) {
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortKey other) {
		if(clickCategoryCount-other.clickCategoryCount!=0) {
			return (int) (clickCategoryCount-other.clickCategoryCount);
		}else if(orderCategoryCount-other.orderCategoryCount!=0) {
			return (int) (orderCategoryCount-other.orderCategoryCount);
		}else if(payCategoryCount-other.payCategoryCount!=0) {
			return (int) (payCategoryCount-other.payCategoryCount);
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey other) {
		if(clickCategoryCount-other.clickCategoryCount!=0) {
			return (int) (clickCategoryCount-other.clickCategoryCount);
		}else if(orderCategoryCount-other.orderCategoryCount!=0) {
			return (int) (orderCategoryCount-other.orderCategoryCount);
		}else if(payCategoryCount-other.payCategoryCount!=0) {
			return (int) (payCategoryCount-other.payCategoryCount);
		}
		return 0;
	}

	public Long getClickCategoryCount() {
		return clickCategoryCount;
	}

	public void setClickCategoryCount(Long clickCategoryCount) {
		this.clickCategoryCount = clickCategoryCount;
	}

	public Long getOrderCategoryCount() {
		return orderCategoryCount;
	}

	public void setOrderCategoryCount(Long orderCategoryCount) {
		this.orderCategoryCount = orderCategoryCount;
	}

	public Long getPayCategoryCount() {
		return payCategoryCount;
	}

	public void setPayCategoryCount(Long payCategoryCount) {
		this.payCategoryCount = payCategoryCount;
	}
	
	

}
