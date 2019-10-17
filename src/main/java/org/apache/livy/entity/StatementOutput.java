package org.apache.livy.entity;

import lombok.Data;

@Data
public class StatementOutput {
	/**
	 * Execution status
	 */
	private String status;
	/**
	 * A monotonically increasing number
	 */
	private int execution_count;
	/**
	 * Statement output, An object mapping a mime type to the result. 
	 * If the mime type is ``application/json``, the value is a JSON value.
	 */
	private String data;
	
    public boolean isError() {
        return this.status.equals("error");
    }
}
