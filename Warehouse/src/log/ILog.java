package log;

import java.sql.SQLException;


import config.Configuration;

public interface ILog {

	/**
	 * Start download file fron FtpServer
	 * 
	 * @param conf
	 * @param log
	 * @throws SQLException
	 */
	void logStartDownloadFTPFeed(Configuration conf, Log log) throws SQLException;

	/**
	 * After download file from FtpServer Success
	 * 
	 * @param conf
	 * @param log
	 * @throws SQLException
	 */
	void logAfterDownloadFTPFeed(Configuration conf, Log log) throws SQLException;

	/**
	 * Start extract CSV from Local to Staging
	 * 
	 * @param conf
	 * @param log
	 * @throws SQLException
	 */
	void logStartExtractToStaging(Configuration conf, Log log) throws SQLException;

	/**
	 * After extract CSV from local to Staging Success
	 * 
	 * @param conf
	 * @param log
	 * @param numRecords
	 * @throws SQLException
	 */
	void logAfterExtractToStaging(Configuration conf, Log log, int numRecords) throws SQLException;

	/**
	 * Start load data from Staging to Warehouse
	 * 
	 * @param conf
	 * @param log
	 * @throws SQLException
	 */
	void logStartLoadToDataWarehouse(Configuration conf, Log log) throws SQLException;

	/**
	 * After load data from Staging to Warehouse success
	 * 
	 * @param conf
	 * @param log
	 * @param numOfRecords
	 * @throws SQLException
	 */
	void logAfterLoadToDataWarehouse(Configuration conf, Log log, int numOfRecords, int numRecordsUp) throws SQLException;

	/**
	 * Close Connection to CONTROL
	 * 
	 * @throws SQLException
	 */
	void close() throws SQLException;

}
