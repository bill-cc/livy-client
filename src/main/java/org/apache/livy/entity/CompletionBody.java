package org.apache.livy.entity;

import org.apache.livy.enums.ESessionKind;

import lombok.Data;

@Data
public class CompletionBody {
	/**
	 * The code for which completion proposals are requested
	 */
	private String code;
	/**
	 * The kind of code to execute
	 */
	private ESessionKind kind;
	/**
	 * cursor position to get proposals
	 */
	private String cursor;
}
