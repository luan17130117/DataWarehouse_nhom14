package log;

import java.util.Date;

public class Log {
	private int id;
    private int idConfig;
    private String sourceFeed;
    private String actionType;
	private String logStatus;
	private String feedName;
    private Date dateUserInsertLog;
    
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getIdConfig() {
		return idConfig;
	}
	public void setIdConfig(int idConfig) {
		this.idConfig = idConfig;
	}
	public String getSourceFeed() {
		return sourceFeed;
	}
	public void setSourceFeed(String sourceFeed) {
		this.sourceFeed = sourceFeed;
	}
	public String getActionType() {
		return actionType;
	}
	public void setActionType(String actionType) {
		this.actionType = actionType;
	}
	public String getStatus() {
		return logStatus;
	}
	public void setLogStatus(String logStatus) {
		this.logStatus = logStatus;
	}

	public void setFeedName(String feedName) {
		this.feedName = feedName;
	}

	public String getFeedName() {
		return feedName;
	}
	
	public void setDateUserInsertLog(Date dateUserInsertLog) {
		this.dateUserInsertLog = dateUserInsertLog;
	}
	
	public Date getDateUserInsertLog() {
		return dateUserInsertLog;
	}
}
