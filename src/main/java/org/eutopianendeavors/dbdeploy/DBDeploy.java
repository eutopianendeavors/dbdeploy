package org.eutopianendeavors.dbdeploy;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;
import org.eutopianendeavors.dbdeploy.models.SqlScript;

public class DBDeploy {

	static final Logger logger = Logger.getLogger(DBDeploy.class);

	private String jdbcDriver;
	private String dbUrl;

	// Database credentials
	private String dbUser;
	private String dbPassword;

	private String scriptsPath;

	public void runScripts() throws IOException, SQLException {
		Connection connection = null;
		SqlScript runningSqlScript = null;
		try {
			connection = DriverManager.getConnection(this.dbUrl, this.dbUser,
					this.dbPassword);
			connection.setAutoCommit(false);
			Integer currentVersion = getCurrentVersion(connection);
			List<SqlScript> upgradeScripts = getSortedUpgradeScripts();

			for (SqlScript sqlScript : upgradeScripts) {
				runningSqlScript = sqlScript;
				if (runningSqlScript.getRunOrder() > currentVersion) {
					SQLRunner runner = new SQLRunner(connection);
					Reader reader = null;
					reader = new FileReader(runningSqlScript.getPath());

					runner.runScript(reader);
				}
			}

		} catch (SQLException e) {
			logger.error("SQL Script at " + runningSqlScript.getPath()
					+ " failed.  Rolling back transaction.", e);
			try {
				connection.rollback();
			} catch (SQLException e2) {
				logger.error("Rollback Failed.", e2);
			}
			throw e;
		}

	}

	private Integer getCurrentVersion(Connection connection)
			throws SQLException {
		Statement statement = null;
		statement = connection.createStatement();
		String sql = "SELECT MAX(version) AS version FROM data_base_version";
		ResultSet resultSet = null;
		Integer version = null;
		try {
			resultSet = statement.executeQuery(sql);
		} catch (SQLException e) {
			logger.debug("Database in initial state.");
			return new Integer(0);
		}
		try {
			if (resultSet.next()) {
				version = new Integer(resultSet.getInt("version"));
			}
		} finally {
			if (resultSet != null) {
				resultSet.close();
			}
			if (statement != null) {
				statement.close();
			}
		}
		return version;
	}

	private List<SqlScript> getSortedUpgradeScripts() {
		List<SqlScript> upgradeScripts = new ArrayList<SqlScript>();
		File dir = new File(this.getClass().getResource(this.scriptsPath)
				.getPath());
		for (File file : dir.listFiles()) {
			Integer runOrder = new Integer(file.getName().split("\\.")[0]);
			String path = file.getPath();
			SqlScript sqlScript = new SqlScript(runOrder, path);
			upgradeScripts.add(sqlScript);
		}
		Collections.sort(upgradeScripts, new Comparator<SqlScript>() {
			@Override
			public int compare(SqlScript sqlScript1, SqlScript sqlScript2) {
				return sqlScript1.getRunOrder().compareTo(
						sqlScript2.getRunOrder());
			}
		});
		return upgradeScripts;
	}

	public String getJdbcDriver() {
		return jdbcDriver;
	}

	public void setJdbcDriver(String jdbcDriver) {
		this.jdbcDriver = jdbcDriver;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPassword() {
		return dbPassword;
	}

	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getScriptsPath() {
		return scriptsPath;
	}

	public void setScriptsPath(String scriptsPath) {
		this.scriptsPath = scriptsPath;
	}

}
