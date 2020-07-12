package connection;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.Configuration;
import log.Log;

public abstract class DBConnection {

	protected String url = "";
	protected String user = "root";
	protected String pass = "mysql@rootuser2019###tracking";

	public DBConnection() {

	}

	public DBConnection(String url, String user, String pass) {
		this.url = url;
		this.user = user;
		this.pass = pass;
	}

	public Connection getConn() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, pass);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public List<Configuration> loadAllConfs() throws SQLException {

		List<Configuration> outputs = new ArrayList<Configuration>();
		Connection conn = getConn();
		String sqlSelectAllConf = "SELECT * FROM data_file_configuration WHERE IS_ACTIVE<>0";
		PreparedStatement ps = conn.prepareStatement(sqlSelectAllConf);
		ResultSet rs = ps.executeQuery();

		while (rs.next()) {
			Configuration conf = new Configuration();
			conf.setIdConf(rs.getInt("ID"));
			conf.setRemoteDir(rs.getString("REMOTE_DIR"));
			conf.setLocalDir(rs.getString("LOCAL_DIR"));
			conf.setHostName(rs.getString("HOST_NAME"));
			conf.setPort(rs.getInt("PORT"));
			conf.setUserHost(rs.getString("USER_HOST"));
			conf.setUserPass(rs.getString("USER_PASS"));
			conf.setColumsFeed(rs.getString("COLUMNS_FEED"));
			conf.setPropsForStaging(rs.getString("PROPS_FOR_STAGING"));
			conf.setPropsStagingForWareHouse(rs.getString("PROPS_STAGING_FOR_WAREHOUSE"));
			conf.setPropsWarehouse(rs.getString("PROPS_WAREHOUSE"));
			conf.setFeedDelimiter(rs.getString("FEED_DELIM"));
			conf.setSrcFeed(rs.getString("SRC_FEED"));
			conf.setTableStaging(rs.getString("TABLE_STAGING"));
			outputs.add(conf);
		}
		close(conn);
		return outputs;
	}

	public List<Log> getLogs(String sql) throws SQLException {
		
		List<Log> lstLogs = new ArrayList<>();
		Connection conn = getConn();
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		
		while (rs.next()) {
			Log log = new Log();
			log.setId(rs.getInt("ID"));
			log.setIdConfig(rs.getInt("ID_CONF"));
			log.setActionType(rs.getString("ACTION_TYPE"));
			log.setSourceFeed(rs.getString("SOURCE_FEED"));
			log.setLogStatus(rs.getString("LOG_STATUS"));
			log.setFeedName(rs.getString("FEED_NAME"));
			lstLogs.add(log);
		}
		close(conn);
		return lstLogs;
	}
}
