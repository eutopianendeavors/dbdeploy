package org.eutopianendeavors.dbdeploy.models;

import org.apache.log4j.Logger;

public class SqlScript {

	static final Logger logger = Logger.getLogger(SqlScript.class);

	private Integer runOrder;
	private String path;

	public SqlScript(Integer runOrder, String path) {
		super();
		this.runOrder = runOrder;
		this.path = path;
	}

	public Integer getRunOrder() {
		return runOrder;
	}

	public String getPath() {
		return path;
	}

}
