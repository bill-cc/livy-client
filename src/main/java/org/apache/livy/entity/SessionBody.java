package org.apache.livy.entity;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class SessionBody {
	/**
	 * The session kind
	 */
	private String kind;
	/**
	 * User to impersonate when starting the session
	 */
	private String proxyUser;
	/**
	 * jars to be used in this session
	 */
	private List<String> jars;
	/**
	 * Python files to be used in this session
	 */
	private List<String> pyFiles;
	/**
	 * files to be used in this session
	 */
	private List<String> files;
	/**
	 * Amount of memory to use for the driver process
	 */
	private String driverMemory;
	/**
	 * Number of cores to use for the driver process
	 */
	private int driverCores;
	/**
	 * Amount of memory to use per executor process
	 */
	private String executorMemory;
	/**
	 * Number of cores to use for each executor
	 */
	private int executorCores;
	/**
	 * Number of executors to launch for this session
	 */
	private int numExecutors;
	/**
	 * Archives to be used in this session
	 */
	private List<String> archives;
	/**
	 * The name of the YARN queue to which submitted
	 */
	private String queue;
	/**
	 * The name of this session
	 */
	private String name;
	/**
	 * Spark configuration properties
	 */
	private Map<String, Object> conf;
	/**
	 * Timeout in second to which session be orphaned
	 */
	private int heartbeatTimeoutInSecond;
}
