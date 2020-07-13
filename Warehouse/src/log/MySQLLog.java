package log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import connection.MySQLConnection;
import constant.Action;
import constant.Result;
import config.Configuration;

public class MySQLLog implements ILog {

	private Connection ctConn;

	public MySQLLog() {
		ctConn = new MySQLConnection().getConn();
	}

	@Override
	public void logStartDownloadFTPFeed(Configuration conf, Log log) throws SQLException {

		String sqlLog = "UPDATE data_file_log SET ACTION_TYPE=?, LOG_STATUS=?, DATE_DOWNLOAD_FTP=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Action.FTP_DOWNLOAD.name().toLowerCase());
		ps.setString(2, Result.FAILED.name().toLowerCase());
		ps.setString(3, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		ps.setInt(4, conf.getIdConf());
		ps.setString(5, log.getFeedName());

		ps.execute();
	}

	@Override
	public void logAfterDownloadFTPFeed(Configuration conf, Log log) throws SQLException {

		String sqlLog = "UPDATE data_file_log SET LOG_STATUS=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Result.SUCCESS.name().toLowerCase());
		ps.setInt(2, conf.getIdConf());
		ps.setString(3, log.getFeedName());

		ps.execute();

	}

	@Override
	public void logStartExtractToStaging(Configuration conf, Log log) throws SQLException {

		String sqlLog = "UPDATE data_file_log SET ACTION_TYPE=?, LOG_STATUS=?, DATE_LOAD_STAGING=?, ROWS_EXTRACT_STAGING=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Action.FILE_TO_STAGING.name().toLowerCase());
		ps.setString(2, Result.FAILED.name().toLowerCase());
		ps.setString(3, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		ps.setInt(4, 0);
		ps.setInt(5, conf.getIdConf());
		ps.setString(6, log.getFeedName());

		ps.execute();
	}

	@Override
	public void logAfterExtractToStaging(Configuration conf, Log log, int numRecords) throws SQLException {

		String sqlLog = "UPDATE data_file_log SET LOG_STATUS=?, ROWS_EXTRACT_STAGING=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Result.SUCCESS.name().toLowerCase());
		ps.setInt(2, numRecords);
		ps.setInt(3, conf.getIdConf());
		ps.setString(4, log.getFeedName());

		ps.execute();
	}

	@Override
	public void logStartLoadToDataWarehouse(Configuration conf, Log log) throws SQLException {
		
		String sqlLog = "UPDATE data_file_log SET ACTION_TYPE=?, LOG_STATUS=?, DATE_LOAD_WAREHOUSE=?, ROWS_LOAD_WAREHOUSE=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Action.LOAD_WAREHOUSE.name().toLowerCase());
		ps.setString(2, Result.FAILED.name().toLowerCase());
		ps.setString(3, new Timestamp(System.currentTimeMillis()).toString().substring(0, 19));
		ps.setInt(4, 0);
		ps.setInt(5, conf.getIdConf());
		ps.setString(6, log.getFeedName());

		ps.execute();

	}

	@Override
	public void logAfterLoadToDataWarehouse(Configuration conf, Log log, int numOfRecords, int numRecordsUp) throws SQLException {
		
		String sqlLog = "UPDATE data_file_log SET LOG_STATUS=?, ROWS_LOAD_WAREHOUSE=?, ROWS_WAREHOUSE_UPDATE=? WHERE ID_CONF=? AND FEED_NAME=?";
		PreparedStatement ps = ctConn.prepareStatement(sqlLog);

		ps.setString(1, Result.SUCCESS.name().toLowerCase());
		ps.setInt(2, numOfRecords);
		ps.setInt(3, numRecordsUp);
		ps.setInt(4, conf.getIdConf());
		ps.setString(5, log.getFeedName());
		

		ps.execute();

	}

	@Override
	public void close() throws SQLException {
		if (null != ctConn) {
			ctConn.close();
		}
	}
}
