package com.maritatf;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Enumeration;
import java.util.Properties;

public class AbstractTestCase extends TestCase {

  public static final String PROP_DB_URL    = "db.url";
  public static final String PROP_DB_DRIVER = "db.driver";
  public static final String PROP_DB_USER   = "db.username";
  public static final String PROP_DB_PASS   = "db.password";

  private static Log logger =
    LogFactory.getLog(AbstractTestCase.class);

  public AbstractTestCase(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
    loadProperties();
  }

  public void loadProperties() throws Exception {
    Properties p = new Properties();
    logger.debug("getting properties from: "+getClass().getName()+".properties");
    try {
      p.load(getClass().getResourceAsStream
             (getClass().getName()+".properties"));
      Enumeration e = p.propertyNames();
      while (e.hasMoreElements()) {
        String name = (String) e.nextElement();
        String value = p.getProperty(name);
        System.setProperty(name, value);
      }
    }
    catch (Exception e) {
      logger.warn("Cannot find properties file");
    }
  }
}
