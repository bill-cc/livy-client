package org.apache.livy.entity;

import java.util.List;

import lombok.Data;

@Data
public class StatementList {
	/**
	 * The statement list
	 */
	private List<StatementInfo> statements;
}
