package org.apache.livy.entity;

import java.util.List;
import java.util.Map;

import org.apache.livy.enums.ESessionKind;
import org.apache.livy.enums.ESessionState;

import lombok.Data;

@Data
public class SessionInfo {
	/**
	 * The session id
	 */
	private int id;
	/**
	 * The application id of this session
	 */
	private String appId;
	/**
	 * Remote user who submitted this session
	 */
	private String owner;
	/**
	 * User to impersonate when running
	 */
	private String proxyUser;
	/**
	 * Session kind (spark, pyspark, sparkr or sql)
	 */
	private ESessionKind kind;
	/**
	 * The log lines
	 */
	private List<String> log;
	/**
	 * The session state
	 */
	private ESessionState state;
	/**
	 * The detailed application info
	 */
	private Map<String, String> appInfo;
	
    public boolean isReady() {
        return ESessionState.IDLE.equals(state);
    }

    public boolean isFinished() {
        return ESessionState.ERROR.equals(state) ||
                ESessionState.DEAD.equals(state) ||
                ESessionState.SUCCESS.equals(state) ||
                ESessionState.KILLED.equals(state) ||
                ESessionState.SHUTTING_DOWN.equals(state);
    }
}
