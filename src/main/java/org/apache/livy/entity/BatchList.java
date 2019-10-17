package org.apache.livy.entity;

import java.util.List;

import lombok.Data;

@Data
public class BatchList {
	/**
	 * The start index to fetch sessions
	 */
	private int from;
	/**
	 * Number of sessions to fetch
	 */
	private int total;
	/**
	 * Batch list
	 */
	private List<BatchInfo> sessions;
}
