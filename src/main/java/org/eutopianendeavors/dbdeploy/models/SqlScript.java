package org.eutopianendeavors.dbdeploy.models;

public class SqlScript {
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
