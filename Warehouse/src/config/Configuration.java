package config;

public class Configuration {
	private int idConf;
	private String remoteDir;
	private String localDir;
	private String hostName;
	private int port;
	private String userHost;
	private String userPass;
	private String columsFeed;
	private String propsForStaging;
	private String propsStagingForWareHouse;
	private String propsWarehouse;
	private String feedDelimiter;
	private String srcFeed;
	private String tableStaging;

	public void setIdConf(int idConf) {
		this.idConf = idConf;
	}

	public int getIdConf() {
		return idConf;
	}

	public String getRemoteDir() {
		return remoteDir;
	}

	public void setRemoteDir(String remoteDir) {
		this.remoteDir = remoteDir;
	}

	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}

	public String getLocalDir() {
		return localDir;
	}

	public String getHostName() {
		return hostName;
	}

	public void setHostName(String hostName) {
		this.hostName = hostName;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserHost() {
		return userHost;
	}

	public void setUserHost(String userHost) {
		this.userHost = userHost;
	}

	public String getUserPass() {
		return userPass;
	}

	public void setUserPass(String userPass) {
		this.userPass = userPass;
	}

	public void setColumsFeed(String columsFeed) {
		this.columsFeed = columsFeed;
	}

	public String getColumsFeed() {
		return columsFeed;
	}

	public void setPropsForStaging(String propsForStaging) {
		this.propsForStaging = propsForStaging;
	}

	public String getPropsForStaging() {
		return propsForStaging;
	}

	public void setPropsStagingForWareHouse(String propsStagingForWareHouse) {
		this.propsStagingForWareHouse = propsStagingForWareHouse;
	}

	public String getPropsStagingForWareHouse() {
		return propsStagingForWareHouse;
	}

	public String getPropsWarehouse() {
		return propsWarehouse;
	}

	public void setPropsWarehouse(String propsWarehouse) {
		this.propsWarehouse = propsWarehouse;
	}

	public String getFeedDelimiter() {
		return feedDelimiter;
	}

	public void setFeedDelimiter(String feedDelimiter) {
		this.feedDelimiter = feedDelimiter;
	}

	public String getSrcFeed() {
		return srcFeed;
	}

	public void setSrcFeed(String srcFeed) {
		this.srcFeed = srcFeed;
	}

	public String getTableStaging() {
		return tableStaging;
	}

	public void setTableStaging(String tableStaging) {
		this.tableStaging = tableStaging;
	}
}
