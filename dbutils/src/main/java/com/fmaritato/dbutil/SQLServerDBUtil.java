package com.fmaritato.dbutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class SQLServerDBUtil extends DBUtil {

  private static Log logger =
    LogFactory.getLog(SQLServerDBUtil.class);

  public SQLServerDBUtil() {
    super();
  }

    public void dumpDDL(String schema, String obj, String type, String dir) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void dumpDDL(String schema, String dir) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void dumpView(String viewName) {
    Connection c = null;
    Statement stmt = null;
    try {
      c = getConnection();
      stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery("sp_helptext '"+
                                       viewName+"'");
      while (rs.next()) {
        String txt = rs.getString(1);
        System.out.println(txt);
      }
    }
    catch (Exception e) {
      logger.error( e.getMessage(), e);
    }
    finally {
      if (stmt != null) { try { stmt.close(); } catch (Exception e) { } }
      if (c != null) { try { c.close(); } catch (Exception e) { } }
    }
  }

  public void listStoredProcs() {
    Connection c = null;
    Statement stmt = null;
    try {
      c = getConnection();
      stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery
        ("select name StoredProcedures from sysobjects where xtype = 'P' order by name");
      while (rs.next()) {
        String txt = rs.getString(1);
        System.out.println(txt);
      }
    }
    catch (Exception e) {
      logger.error( e.getMessage(), e);
    }
    finally {
      if (stmt != null) { try { stmt.close(); } catch (Exception e) { } }
      if (c != null) { try { c.close(); } catch (Exception e) { } }
    }
  }
  public void dumpStoredProcedure(String procName) {
    Connection c = null;
    Statement stmt = null;
    try {
      c = getConnection();
      stmt = c.createStatement();
      ResultSet rs = stmt.executeQuery("sp_helptext '"+
                                       procName+"'");
      while (rs.next()) {
        String txt = rs.getString(1);
        System.out.println(txt);
      }
    }
    catch (Exception e) {
      logger.error( e.getMessage(), e);
    }
    finally {
      if (stmt != null) { try { stmt.close(); } catch (Exception e) { } }
      if (c != null) { try { c.close(); } catch (Exception e) { } }
    }
  }
}

