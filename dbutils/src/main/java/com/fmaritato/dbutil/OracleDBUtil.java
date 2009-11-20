package com.fmaritato.dbutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class OracleDBUtil extends DBUtil {

    private static Log logger =
            LogFactory.getLog(OracleDBUtil.class);

    public OracleDBUtil() {
        super();
    }

    public void dumpDDL(String schema,
                        String object,
                        String type,
                        String directory) throws Exception {

        Connection c = null;
        PreparedStatement stmt = null;
        try {
            c = getConnection();
            stmt = c.prepareStatement("select DBMS_METADATA.GET_DDL(?,?) from dual");
            stmt.setString(1, type);
            stmt.setString(2, object);
            ResultSet rs = stmt.executeQuery();

            File f = new File(directory + File.separator +
                              type.toLowerCase());
            if (!f.exists()) {
                f.mkdirs();
            }
            Writer out = new BufferedWriter
                    (new FileWriter(f.getAbsolutePath() + File.separator +
                                    object.toLowerCase() + ".sql"));
            printHeader(out, getFrom(type), getWhere(type), object);
            getFormatter().format(rs, out, false);
            printFooter(out);
            out.flush();
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        finally {
            if (stmt != null) {
                stmt.close();
            }
            if (c != null) {
                c.close();
            }
        }
    }

    public void dumpDDL(String schema, String dir) throws Exception {
        Connection c = null;
        try {
            c = getConnection();
            logger.info("Dumping tables");
            dumpTables(c, dir);
            logger.info("Dumping sequences");
            dumpSequences(c, dir);
            logger.info("Dumping views");
            dumpViews(c, dir);
            logger.info("Dumping triggers");
            dumpTriggers(c, dir);
        }
        catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        finally {
            if (c != null) {
                c.close();
            }
        }
    }

    public void dumpSequences(Connection c, String dir) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = c.prepareStatement("select sequence_name from user_sequences");
            rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("sequence_name");
                logger.debug("sequence: " + name);
                dumpDDL(null, name, "SEQUENCE", dir);
            }
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public void dumpTables(Connection c, String dir) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = c.prepareStatement("select table_name from user_tables");
            rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("table_name");
                logger.debug("table: " + name);
                if (name.indexOf("$") < 0 &&
                    !name.startsWith("SYS")) {
                    dumpDDL(null, name, "TABLE", dir);
                }
                else {
                    logger.info("skipping table: " + name);
                }
            }
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public void dumpViews(Connection c, String dir) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = c.prepareStatement("select view_name from user_views");
            rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("view_name");
                logger.debug("view: " + name);
                if (name.indexOf("$") < 0 &&
                    !name.startsWith("SYS")) {
                    dumpDDL(null, name, "VIEW", dir);
                }
                else {
                    logger.info("skipping table: " + name);
                }
            }
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public void dumpTriggers(Connection c, String dir) throws Exception {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = c.prepareStatement("select trigger_name from user_triggers");
            rs = stmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("trigger_name");
                logger.debug("trigger: " + name);
                dumpDDL(null, name, "TRIGGER", dir);
            }
        }
        finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    private String getFrom(String type) {
        if ("TABLE".equals(type)) {
            return "user_tables";
        }
        else if ("SEQUENCE".equals(type)) {
            return "user_sequences";
        }
        else if ("VIEW".equals(type)) {
            return "user_views";
        }
        else if ("TRIGGER".equals(type)) {
            return "user_Triggers";
        }
        return null;
    }

    private String getWhere(String type) {
        if ("TABLE".equals(type)) {
            return "table_name";
        }
        else if ("SEQUENCE".equals(type)) {
            return "sequence_name";
        }
        else if ("VIEW".equals(type)) {
            return "view_name";
        }
        else if ("TRIGGER".equals(type)) {
            return "trigger_name";
        }
        return null;
    }

    public void printHeader(Writer out, String from, String where, String obj)
            throws IOException {
        String header = "declare\n" +
                        "  c number := 0;\n" +
                        "begin\n" +
                        "  select count(*) into c\n" +
                        "  from " + from + "\n" +
                        "  where " + where + " = '" + obj + "';\n" +
                        "  if c=0 then\n" +
                        "    execute immediate '\n";
        out.write(header);
    }

    public void printFooter(Writer out) throws IOException {
        out.write("';\n  end if;\nend;\n/\n");
    }

    public void dumpView(String viewName) {
        query("select DBMS_METADATA.GET_DDL('VIEW','" + viewName.toUpperCase() + "') from dual");
    }

    public void listStoredProcs() {
        query("select procedure_name from user_procedures");
    }

    public void dumpStoredProcedure(String procName) {
        query("select text from user_source where name=upper('" + procName + "')");
    }
}

