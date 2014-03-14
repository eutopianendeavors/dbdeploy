package org.eutopianendeavors.dbdeploy;

import java.io.IOException;
import java.sql.SQLException;

import junit.framework.TestCase;

public class DBDeployTest extends TestCase {

	public void testDBDeploy() throws IOException, SQLException {
		DBDeploy dbDeploy = new DBDeploy();
		dbDeploy.setDbUrl("jdbc:mysql://localhost:3306/social");
		dbDeploy.setJdbcDriver("com.mysql.jdbc.Driver");
		dbDeploy.setDbUser("social");
		dbDeploy.setDbPassword("social");
		dbDeploy.setScriptsPath("/dbdeploy/scripts");
		dbDeploy.runScripts();
	}
}
