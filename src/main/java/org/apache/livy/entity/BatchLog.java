package org.apache.livy.entity;

import java.util.List;
import java.util.stream.Collectors;

import lombok.Data;

@Data
public class BatchLog {
	/**
	 * The session id
	 */
	private int id;
	/**
	 * Offset from start of log
	 */
	private int from;
	/**
	 * Max number of log lines
	 */
	private int size;
	/**
	 * The log lines
	 */
	private List<String> log;
	
    public String logs() {
        return log.stream().collect(Collectors.joining("\n"));
    }
}
