package org.eutopianendeavors.dbdeploy;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class SQLRunner {
	static Logger logger = Logger.getLogger(SQLRunner.class);
	public static final String DELIMITER_LINE_REGEX = "(?i)DELIMITER.+",
			DELIMITER_LINE_SPLIT_REGEX = "(?i)DELIMITER",
			DEFAULT_DELIMITER = ";";
	private final Connection connection;
	private String delimiter = SQLRunner.DEFAULT_DELIMITER;

	public SQLRunner(final Connection connection) {
		if (connection == null) {
			throw new RuntimeException("SqlRunner requires an SQL Connection");
		}
		this.connection = connection;
	}

	public void runScript(final Reader reader) throws SQLException, IOException {
		this.runScript(this.connection, reader);
	}

	private void runScript(final Connection conn, final Reader reader)
			throws SQLException, IOException {
		StringBuffer command = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			final LineNumberReader lineReader = new LineNumberReader(reader);
			String line = null;
			while ((line = lineReader.readLine()) != null) {
				if (command == null) {
					command = new StringBuffer();
				}
				String trimmedLine = line.trim();

				if (trimmedLine.startsWith("--")
						|| trimmedLine.startsWith("//")
						|| trimmedLine.startsWith("#")) {

					// Line is a comment
					logger.debug(trimmedLine);

				} else if (trimmedLine.endsWith(this.delimiter)) {

					// Line is end of statement

					// Support new delimiter
					final Pattern pattern = Pattern
							.compile(SQLRunner.DELIMITER_LINE_REGEX);
					final Matcher matcher = pattern.matcher(trimmedLine);
					if (matcher.matches()) {
						delimiter = trimmedLine
								.split(SQLRunner.DELIMITER_LINE_SPLIT_REGEX)[1]
								.trim();

						// New delimiter is processed, continue on next
						// statement
						line = lineReader.readLine();
						if (line == null) {
							break;
						}
						trimmedLine = line.trim();
					}

					// Append
					command.append(line.substring(0,
							line.lastIndexOf(this.delimiter)));
					command.append(" ");

					stmt = conn.createStatement();
					logger.debug("");
					logger.debug(command);
					boolean hasResults = false;
					hasResults = stmt.execute(command.toString());
					rs = stmt.getResultSet();
					if (hasResults && rs != null) {

						// Print result column names
						final ResultSetMetaData md = rs.getMetaData();
						final int cols = md.getColumnCount();
						for (int i = 0; i < cols; i++) {
							final String name = md.getColumnLabel(i + 1);
							logger.debug(name + "\t");
						}
						logger.debug("");
						logger.debug(StringUtils.repeat("---------",
								md.getColumnCount()));

						// Print result rows
						while (rs.next()) {
							for (int i = 1; i <= cols; i++) {
								final String value = rs.getString(i);
								logger.debug(value + "\t");
							}
							logger.debug("");
						}
					} else {
						logger.debug("Updated: " + stmt.getUpdateCount());
					}
					if (rs != null) {
						rs.close();
					}
					if (stmt != null) {
						stmt.close();
					}
					command = null;
				} else {

					// Line is middle of a statement

					// Support new delimiter
					final Pattern pattern = Pattern
							.compile(SQLRunner.DELIMITER_LINE_REGEX);
					final Matcher matcher = pattern.matcher(trimmedLine);
					if (matcher.matches()) {
						delimiter = trimmedLine
								.split(SQLRunner.DELIMITER_LINE_SPLIT_REGEX)[1]
								.trim();
						line = lineReader.readLine();
						if (line == null) {
							break;
						}
						trimmedLine = line.trim();
					}
					command.append(line);
					command.append(" ");
				}
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}
}