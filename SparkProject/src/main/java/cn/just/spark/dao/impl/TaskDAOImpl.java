package cn.just.spark.dao.impl;

import java.sql.ResultSet;

import cn.just.spark.dao.ITaskDAO;
import cn.just.spark.domain.Task;
import cn.just.spark.jdbc.JDBCHelper;
import cn.just.spark.jdbc.JDBCHelper.QueryCallBack;
/**
 * TaskDAO实现类
 * 2018-7-31
 * @author shinelon
 *
 */
public class TaskDAOImpl implements ITaskDAO {

	@Override
	public Task findById(long taskId) {
		final Task task=new Task();
		JDBCHelper jdbcHelper=JDBCHelper.getInstance();
		String sql="select * from task where task_id=?";
		Object[] params=new Object[] {taskId};
		jdbcHelper.executeQuery(sql, params, new QueryCallBack() {
			@Override
			public void processor(ResultSet rs) throws Exception {
				if(rs.next()) {
					long taskId=rs.getLong(1);
					String taskName=rs.getString(2);
					String createTime=rs.getString(3);
					String startTime=rs.getString(4);
					String finishTime=rs.getString(5);
					String taskType=rs.getString(6);
					String taskStatus=rs.getString(7);
					String taskParam=rs.getString(8);
					task.setTaskId(taskId);
					task.setTaskName(taskName);
					task.setCreateTime(createTime);
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam);
				}
			}
		});
		return task;
	}

}
