package org.apache.livy.entity;

import lombok.Data;

@Data
public class SessionState {
	/**
	 * Session id
	 */
	private int id;
	/**
	 * The current state of session
	 */
	private String state;
}
