package cn.just.spark.test;

import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.dao.impl.DAOFactory;
import cn.just.spark.domain.Task;

/**
 * DAO测试类
 * 2018-7-31
 * @author shinelon
 *
 */
public class TaskDAOTest {
	public static void main(String[] args) {
		ITaskDAO taskDAO=DAOFactory.getTaskDAO();
		Task task=taskDAO.findById(1);
		System.out.println(task.getTaskName());
	}

}
