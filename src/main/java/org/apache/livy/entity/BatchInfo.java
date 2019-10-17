package org.apache.livy.entity;

import java.util.List;
import java.util.Map;

import org.apache.livy.enums.ESessionState;

import lombok.Data;

/**
 * 
 * 
 * @author cheng
 *
 */
@Data
public class BatchInfo {
	/**
	 * The session id
	 */
	private int id;
	/**
	 * The application id of this session
	 */
	private String appId;
	/**
	 * The detailed application info
	 */
	private Map<String, String> appInfo;
	/**
	 * The log lines
	 */
	private List<String> log;
	/**
	 * The batch state string
	 */
	private ESessionState state;
	
    public boolean isRunning() {
        return ESessionState.RUNNING.equals(state);
    }
	
    public boolean isFinished() {
        return ESessionState.ERROR.equals(state) ||
                ESessionState.DEAD.equals(state) ||
                ESessionState.SUCCESS.equals(state) ||
                ESessionState.KILLED.equals(state) ||
                ESessionState.SHUTTING_DOWN.equals(state);
    }
}
