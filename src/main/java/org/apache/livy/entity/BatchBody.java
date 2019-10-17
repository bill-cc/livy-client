package org.apache.livy.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class BatchBody {
	/**
	 * File containing the application to execute
	 */
	private String file;
	/**
	 * User to impersonate when running the job
	 */
	private String proxyUser;
	/**
	 * Application Java/Spark main class
	 */
	private String className;
	/**
	 * Command line arguments for the application
	 */
	private List<String> args;
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
	private Map<String, String> conf;
	
	public BatchBody() {
		this.args = new ArrayList<>();
		this.jars = new ArrayList<>();
		this.pyFiles = new ArrayList<>();
		this.files = new ArrayList<>();
		this.archives = new ArrayList<>();
		this.conf = new HashMap<>();
	}
	
	/**
	 * Add arg string
	 * @param arg
	 */
	public void addArg(String arg) {
		this.args.add(arg);
	}
	
	/**
	 * Add jar path string
	 * @param arg
	 */
	public void addJar(String jar) {
		this.jars.add(jar);
	}
	
	/**
	 * Add PY file path string
	 * @param arg
	 */
	public void addPyFile(String pyFile) {
		this.pyFiles.add(pyFile);
	}
	
	/**
	 * Add file uri string
	 * @param arg
	 */
	public void addAttachedFile(String file) {
		this.files.add(file);
	}
	
	/**
	 * Add archive string
	 * @param arg
	 */
	public void addArchive(String archive) {
		this.archives.add(archive);
	}
	
	/**
	 * Add archive string
	 * @param arg
	 */
	public void putConf(String key, String value) {
		this.conf.put(key, value);
	}
}
