package org.apache.livy.entity;

import org.apache.livy.enums.ESessionKind;

import lombok.Data;

@Data
public class StatementBody {

	/**
	 * code
	 */
	private String code;
	/**
	 * session kind
	 */
	private ESessionKind kind;
}
