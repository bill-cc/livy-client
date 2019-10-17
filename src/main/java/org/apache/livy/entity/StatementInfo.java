package org.apache.livy.entity;

import org.apache.livy.enums.EStatementState;

import lombok.Data;

@Data
public class StatementInfo {
	/**
	 * The statement id
	 */
	private int id;
	/**
	 * The execution code
	 */
	private String code;
	/**
	 * The execution state
	 */
	private EStatementState state;
	/**
	 * The execution output
	 */
	private StatementOutput output;
	
	/**
	 * Available
	 * @return
	 */
    public boolean isAvailable() {
        return EStatementState.AVAILABLE.equals(state) || EStatementState.CANCELLED.equals(state);
    }
}
