package org.apache.livy.entity;

import java.util.List;

import lombok.Data;

@Data
public class CompletionList {

	/**
	 * candidate list
	 */
	private List<String> candidates;
}
