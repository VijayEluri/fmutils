package com.maritatf;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class AbstractDBTestCase extends AbstractTestCase {

  private static Log logger =
    LogFactory.getLog(AbstractDBTestCase.class);

  private static final String PROP_TABLE_SQL = "table.sql";

  public AbstractDBTestCase (String testName) {
    super(testName);
  }

  public Connection getConnection() throws Exception {
    final String dbUrl    = System.getProperty(PROP_DB_URL);
    final String dbDriver = System.getProperty(PROP_DB_DRIVER);
    return getConnection(dbDriver, dbUrl, null, null);
  }

  public Connection getConnection(String dbDriver,
                                  String dbUrl,
                                  String username,
                                  String password) throws Exception {
    Class.forName(dbDriver);
    long start = System.currentTimeMillis();
    Connection c = DriverManager.getConnection(dbUrl, username, password);
    long end   = System.currentTimeMillis();
    logger.debug( "getConnection("+dbUrl+"): "+(end-start));
    return c;
  }

  public void createTables() {

    Connection c = null;
    try {
      c = getConnection();
      for (int i=1; ; i++) {
        final String tableSql = System.getProperty(PROP_TABLE_SQL+"."+i);
        if (tableSql == null) {
          break;
        }
        Statement stmt = c.createStatement();
        logger.debug( "executing: "+tableSql);
        long start = System.currentTimeMillis();
        stmt.execute(tableSql);
        long end = System.currentTimeMillis();
        logger.debug( "create time: "+(end-start)+" ms");
      }
    }
    catch (Exception e) {
      logger.error( e.getMessage(), e);
      // don't fail because table may already exist...thats ok
    }
    finally {
      if (c != null) { try { c.close(); } catch (Exception e) {} }
    }
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(AbstractDBTestCase.class);
    return suite;
  }

  public static void main(String[] args) {
    try {
      TestRunner.run(suite());
    }
    catch (Throwable e) {
      logger.error( e.getMessage(), e);
    }
  }
}

