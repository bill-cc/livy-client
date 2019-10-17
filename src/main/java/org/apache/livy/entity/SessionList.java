package org.apache.livy.entity;

import java.util.List;

import lombok.Data;

@Data
public class SessionList {
	/**
	 * The start index to fetch sessions
	 */
	private int from;
	/**
	 * Number of sessions to fetch
	 */
	private int total;
	/**
	 * Session list
	 */
	private List<SessionInfo> sessions;
}
