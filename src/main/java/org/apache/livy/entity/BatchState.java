package org.apache.livy.entity;

import lombok.Data;

@Data
public class BatchState {
	/**
	 * Batch session id
	 */
	private int id;
	/**
	 * The current state of batch session
	 */
	private String state;
}
