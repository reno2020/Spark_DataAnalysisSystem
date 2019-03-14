package cn.just.spark.dao.impl;
/**
 * 获取DAO的工厂类
 * 2018-7-31
 * @author shinelon
 *
 */

import cn.just.spark.dao.IAdBlockListDAO;
import cn.just.spark.dao.IAdStatDAO;
import cn.just.spark.dao.IAdUserClickCountDAO;
import cn.just.spark.dao.IAreaTop3ProductDAO;
import cn.just.spark.dao.IPageSplitConvertRateDAO;
import cn.just.spark.dao.ISessionAggrSataDAO;
import cn.just.spark.dao.ISessionDetailDAO;
import cn.just.spark.dao.ISessionRandomExtractDAO;
import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.dao.ITop10CategoryDAO;
import cn.just.spark.dao.ITop10CategorySessionDAO;

public class DAOFactory {
	/**
	 * 获取taskDAO
	 * @return
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
	/**
	 * 获取用户session聚合信息DAO
	 * @return
	 */
	public static ISessionAggrSataDAO getSessionAggrSataDAO() {
		return new SessionAggrSataDAOImpl();
	}
	/**
	 * 获取随机抽取session信息DAO
	 */
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	/**
	 * 获取session明细信息DAO
	 */
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	/**
	 * 获取top10热门品类DAO
	 */
	public  static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	/**
	 * 获取top10活跃用户DAO
	 */
	public static ITop10CategorySessionDAO getTop10CategorySessionDAO() {
		return new Top10CategorySessionDAOImpl();
	}
	/**
	 * 计算页面切片转换率DAO
	 */
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	/**
	 * 各区域top3热门商品
	 */
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	/**
	 * 用户广告流量实时统计
	 */
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	/**
	 * 动态黑名单生成DAO
	 */
	public static IAdBlockListDAO getAdBlockListDAO() {
		return new AdBlockListDAOImpl();
	}
	/**
	 * 实时统计广告流量
	 */
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}

}
