package cn.just.spark.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import cn.just.spark.conf.ConfigurationManager;
import cn.just.spark.constant.Constants;

/**
 * JDBC辅助组件
 * 使用 单例模式创建，构造器内部创建一个简单的数据库连接池
 * 2018-7-30
 * @author shinelon
 *
 */
public class JDBCHelper {
	private static JDBCHelper instance=null;
	/**
	 * 在静态类中加载JDBC类的驱动
	 */
	static {
		try {
			//防止硬编码从配置文件中读取
			String driver=ConfigurationManager.getProperties(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 使用LinkedList创建一个数据库连接池
	 */
	public LinkedList<Connection>  datasource=new LinkedList<Connection>();
	
	//单例构造体私有化
	private JDBCHelper(){
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		//初始化数据库连接池
		int size=ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
			String url=null;
			String user=null;
			String password=null;
			for(int i=0;i<size;i++) {
				//本地环境测试使用
				if(local) {
					url=ConfigurationManager.getProperties(Constants.JDBC_URL);
					user=ConfigurationManager.getProperties(Constants.JDBC_USER);
					password=ConfigurationManager.getProperties(Constants.JDBC_PASSWORD);
				}else {   //生产环境测试
					url=ConfigurationManager.getProperties(Constants.JDBC_URL_PROD);
					user=ConfigurationManager.getProperties(Constants.JDBC_USER_PROD);
					password=ConfigurationManager.getProperties(Constants.JDBC_PASSWORD_PROD);
				}
				try {
					Connection connection=DriverManager.getConnection(url, user, password);
					datasource.push(connection);
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}
	}
	
	/**
	 * 提供外部调动单例的方法
	 * static使用静态方法，线程安全
	 * @return
	 */
	public static JDBCHelper getInstance(){
		if(instance==null) {
			synchronized (JDBCHelper.class) {
				if(instance==null) {
					instance=new JDBCHelper();
				}
			}
		}
		return instance;
	}
	/**
	 * 获取数据库连接池的连接
	 */
	public synchronized Connection getConnection() {
		while(datasource.size()==0) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}
	
	/**
	 * 定义增删改等操作
	 * @param sql
	 * @param params
	 * @return
	 */
	public int  executeUpdate(String sql,Object[] params) {
		int rnt=0;
		Connection connection=null;
		PreparedStatement psmt=null;
		try {
			connection=getConnection();
			psmt=connection.prepareStatement(sql);
			for(int i=0;i<params.length;i++) {
				psmt.setObject(i+1, params[i]);
			}
			rnt=psmt.executeUpdate();
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			//放入数据库连接池中
			if(connection!=null)
				datasource.push(connection);
		}
		return rnt;
	}
	

	public void executeQuery(String sql,Object[] params,QueryCallBack queryCallBack) {
		Connection connection=null;
		PreparedStatement psmt=null;
		ResultSet rs=null;
		try {
			connection=getConnection();
			psmt=connection.prepareStatement(sql);
			for(int i =0;i<params.length;i++) {
				psmt.setObject(i+1, params[i]);
			}
			rs=psmt.executeQuery();
			queryCallBack.processor(rs);
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(connection!=null)
				datasource.push(connection);
		}
	}
	/**
	 * 定义批量处理的操作
	 * @param sql
	 * @param listParams
	 */
	public void executeBatch(String sql ,List<Object[]> listParams) {
		Connection connection=null;
		PreparedStatement psmt=null;
		try {
			connection=getConnection();
			psmt=connection.prepareStatement(sql);
			//设置自动提交为false
			connection.setAutoCommit(false);
			for(Object[] params:listParams) {
				for(int i=0;i<params.length;i++) {
					psmt.setObject(i+1, params[i]);
				}
				psmt.addBatch();
			}
			//执行
			psmt.executeBatch();
			//手动提交
			connection.commit();
		}catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(connection!=null) {
				datasource.push(connection);
			}
		}
	}
	/**
	 * 定义查询结果回调接口
	 * 使用查询回调接口，可以自由按照自己的业务封装返回结果
	 * @author shinelon
	 *
	 */
	public static interface QueryCallBack{
		/**
		 * 处理查询返回结果
		 * @param rs
		 * @throws Exception
		 */
		void processor(ResultSet rs) throws Exception;
	}
	
	
	
	
	
	
	
	
	
	
	

}
