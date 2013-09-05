package com.fmaritato.dbutil;

import com.fmaritato.dbutil.text.Formatter;
import com.fmaritato.dbutil.text.SimpleFormatter;
import com.fmaritato.dbutil.text.TabularFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Command line DB tool.
 * <p/>
 * Usage: db [properties] [options]
 * <p/>
 * Where:
 * [properties] are:
 * -Ddb.url      db url
 * -Ddb.driver   db driver to use (must be in classpath)
 * -Ddb.username db username
 * -Ddb.password db password
 * <p/>
 * and [options] are:
 * -d <tableName> "describe" the table.
 * -q "<query>"   query to run against the DB.
 * -l             list all tables.
 * -s <procName>  Display the text of the stored procedure.
 * -p <filename>  file containing above properties.
 */
public abstract class DBUtil {

    private final static Log log =
            LogFactory.getLog(DBUtil.class);

    public static final String PROP_DB_URL = "db.url";
    public static final String PROP_DB_DRIVER = "db.driver";
    public static final String PROP_DB_USER = "db.username";
    public static final String PROP_DB_PASS = "db.password";
    public static final String PROP_FETCH_SIZE = "db.fetchSize";

    private Formatter formatter;
    private int fetchSize = 100;
    private Map bindVariables;

    public Map getBindVariables() {
        return bindVariables;
    }

    public void setBindVariables(Map aValue) {
        bindVariables = aValue;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int aValue) {
        fetchSize = aValue;
    }

    public Formatter getFormatter() {
        return formatter;
    }

    public void setFormatter(Formatter aValue) {
        formatter = aValue;
    }

    public DBUtil() {
        try {
            setFetchSize(Integer.parseInt(System.getProperty(PROP_FETCH_SIZE)));
        }
        catch (NumberFormatException e) {
            if (log.isWarnEnabled()) {
                log.warn(e);
            }
        }
    }

    public Connection getConnection() throws Exception {
        final String dbUrl = System.getProperty(PROP_DB_URL);
        final String dbDriver = System.getProperty(PROP_DB_DRIVER);
        final String dbUser = System.getProperty(PROP_DB_USER);
        final String dbPass = System.getProperty(PROP_DB_PASS);
        if (log.isDebugEnabled()) {
            log.debug("dbUrl: " + dbUrl +
                      " dbDriver: " + dbDriver +
                      " dbUser: " + dbUser +
                      " dbPass: " + dbPass);
        }
        return getConnection(dbDriver, dbUrl, dbUser, dbPass);
    }

    public Connection getConnection(String dbDriver,
                                    String dbUrl,
                                    String username,
                                    String password) throws Exception {

        Class.forName(dbDriver);

        Properties props = new Properties();
        props.put("user", username);
        props.put("password", password);
        props.put("SetBigStringTryClob", "true");

        long start = System.currentTimeMillis();
        Connection c = DriverManager.getConnection(dbUrl, props);
        long end = System.currentTimeMillis();
        log.debug("getConnection: " + (end - start));
        return c;
    }

    /**
     * List all tables in the input schema
     */
    public void listAll() {
        Connection c = null;
        try {
            c = getConnection();
            DatabaseMetaData meta = c.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, null);
            Writer out = new BufferedWriter(new OutputStreamWriter(System.out));
            formatter.format(rs, out);
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (c != null) {
                try {
                    c.close();
                }
                catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.warn(e);
                    }
                }
            }
        }
    }

    /**
     * Run a query/update command.
     *
     * @param queryString The SQL query to run.
     */
    public void query(String queryString) {
        Connection c = null;
        PreparedStatement stmt = null;
        try {
            c = getConnection();

            log.debug(queryString);
            stmt = c.prepareStatement(queryString);
            stmt.setFetchSize(getFetchSize());

            if (bindVariables != null && bindVariables.size() > 0) {
                for (Object o : bindVariables.keySet()) {
                    String key = (String) o;
                    int position = Integer.parseInt(key);
                    log.debug("position: " + position);
                    String value = (String) bindVariables.get(key);
                    log.debug("setting value: " + value);
                    stmt.setString(position, value);
                }
            }

            Writer out = new BufferedWriter(new OutputStreamWriter(System.out));
            if (!queryString.toLowerCase().startsWith("select") ||
                queryString.toLowerCase().indexOf("into") > 0) {
                long start = System.currentTimeMillis();
                int num = stmt.executeUpdate();
                long end = System.currentTimeMillis();
                log.debug("update: " + (end - start));
                out.write(num + " records updated\n");
                out.flush();
            }
            else {
                long start = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery();
                long end = System.currentTimeMillis();
                log.debug("query: " + (end - start));
                formatter.format(rs, out);
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (c != null) {
                try {
                    c.close();
                }
                catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Print the column names and data types for the given table.
     *
     * @param tableName The table name to describe
     */
    public void describe(String tableName) {
        if (tableName == null) {
            if (log.isDebugEnabled()) {
                log.debug("TableName is null. Exiting.");
            }
            return;
        }

        String[] tokens = tableName.split("\\.");
        String table = null;
        String schema = null;
        if (log.isDebugEnabled()) {
            log.debug("tableName: " + tableName + " tokens.length: " + tokens.length);
        }

        if (tokens.length == 1) {
            table = tokens[0];
        }
        else if (tokens.length == 2) {
            schema = tokens[0].toUpperCase();
            table = tokens[1];
        }
        if (table == null) {
            if (log.isDebugEnabled()) {
                log.debug("Table is null. Exiting.");
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("table: " + table + " schema: " + schema);
        }

        Connection c = null;
        Writer out = null;
        try {
            c = getConnection();
            DatabaseMetaData meta = c.getMetaData();
            out = new BufferedWriter(new OutputStreamWriter(System.out));
            formatter.format(meta, schema, table, out);
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        finally {
            if (out != null) {
                try {
                    out.close();
                }
                catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
            if (c != null) {
                try {
                    c.close();
                }
                catch (Exception e) {
                    if (log.isWarnEnabled()) {
                        log.warn(e);
                    }
                }
            }
        }
    }

    public void processFile(String filename) {
        try {
            BufferedReader in = new BufferedReader(new FileReader(filename));
            StringBuffer command = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                command.append(line).append(" ");
                if (line.indexOf(";") >= 0) {
                    String cmd = command.toString();
                    log.error(cmd);
                    query(cmd);
                    command.delete(0, command.length());
                }
            }
            if (command.length() > 0) {
                String cmd = command.toString();
                log.error(cmd);
                query(cmd);
            }
        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * List all defined stored procedures by name
     */
    public abstract void listStoredProcs();

    /**
     * Display the text of a stored procedure to stdout.
     *
     * @param procName procedure name
     */
    public abstract void dumpStoredProcedure(String procName);

    /**
     * Display the text of a view
     *
     * @param viewName view name
     */
    public abstract void dumpView(String viewName);

    /**
     * Dump the ddl for a specific object.
     *
     * @param schema the schema to use
     * @param obj    Object name
     * @param type   TABLE, VIEW, etc.
     * @param dir    directory to dump the ddl files
     * @throws Exception if something bad happens...
     */
    public abstract void dumpDDL(String schema, String obj, String type, String dir)
            throws Exception;

    /**
     * Dump DDL for *all* objects in the schema...
     *
     * @param schema schema to dump ddl from
     * @param dir    directory to put files
     * @throws Exception if something bad happens
     */
    public abstract void dumpDDL(String schema, String dir)
            throws Exception;

    public static void main(String[] args) {
        ApplicationContext ctx =
                new ClassPathXmlApplicationContext("spring.cfg.xml");

        Options options = new Options();
        options.addOption("l", false, "List all tables/views");
        options.addOption("f", true, "Set the fetch size");
        options.addOption("d", true, "Describe a table/view");
        options.addOption("q", true, "Execute a query");
        options.addOption("s", true, "Show the text of a stored procedure");
        options.addOption("v", true, "Show the text of a view");
        options.addOption("h", false, "Show this message");
        options.addOption("t", true, "Table name");

        options.addOption(OptionBuilder.withLongOpt("formatter")
                                       .withDescription("Use a different formatter")
                                       .hasArg()
                                       .withValueSeparator()
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("listStoredProcs")
                                       .withDescription("List stored procedures")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("bind")
                                       .hasArg()
                                       .withValueSeparator()
                                       .withDescription("comma separated bind key=val pairs")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("input")
                                       .hasArg()
                                       .withValueSeparator()
                                       .withDescription("file containing sql commands")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("sqlserver")
                                       .withDescription("Force SQLServer implementation")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("sybase")
                                       .withDescription("Force SQLServer implementation")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("oracle")
                                       .withDescription("Force Oracle implementation")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("ddl")
                                       .hasArg()
                                       .withValueSeparator()
                                       .withDescription("dump the ddl for an object,type")
                                       .create());
        options.addOption(OptionBuilder.withLongOpt("help")
                                       .withDescription("Another option to show help msg")
                                       .create());

        DBUtil u;

        CommandLineParser parser = new PosixParser();
        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("sqlserver")) {
                u = (DBUtil) ctx.getBean("SQLServerDBUtil");
            }
            else if (line.hasOption("oracle")) {
                u = (DBUtil) ctx.getBean("OracleDBUtil");
            }
            else if (line.hasOption("sybase")) {
                u = (DBUtil) ctx.getBean("SybaseDBUtil");
            }
            else {
                u = (DBUtil) ctx.getBean("DBUtil");
            }

            /**
             * Options that affect behavior
             */
            if (line.hasOption("f")) {
                u.setFetchSize(Integer.parseInt(line.getOptionValue("f")));
            }


            if (line.hasOption("h") || line.hasOption("help")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("db", options);
            }

            if (line.hasOption("bind")) {
                log.debug("binding...");
                HashMap map = new HashMap();
                String[] pairs = line.getOptionValue("bind").split(",");
                for (String pair : pairs) {
                    String[] keyval = pair.split("=");
                    map.put(keyval[0], keyval[1]);
                }
                log.debug("bind variables: " + map);
                u.setBindVariables(map);
            }

            if (line.hasOption("formatter")) {
                String formatter = line.getOptionValue("formatter");
                if ("csv".equals(formatter)) {
                    u.setFormatter(new SimpleFormatter(","));
                }
                else if ("space".equals(formatter)) {
                    SimpleFormatter f = new SimpleFormatter();
                    f.setDelim(" ");
                    u.setFormatter(f);
                }
                else if ("tab".equals(formatter)) {
                    SimpleFormatter f = new SimpleFormatter();
                    f.setDelim("	");
                    u.setFormatter(f);
                }
                else {
                    // assume its a fully qualified formatter name and use reflection.
                    Class classdef = Class.forName(line.getOptionValue("formatter"));
                    u.setFormatter((Formatter) classdef.newInstance());
                }
                log.debug("using formatter: " + u.getFormatter().getClass().getName());
            }
            else {
                // Set the default formatter otherwise
                u.setFormatter(new TabularFormatter());
            }

            // Set the table name hack...
            if (line.hasOption("t")) {
                u.getFormatter().setTableName(line.getOptionValue("t"));
            }

            /**
             * Commands
             */
            if (line.hasOption("l")) {
                u.listAll();
            }
            else if (line.hasOption("d")) {
                if (u.getFormatter() == null) {
                    u.setFormatter(new SimpleFormatter(","));
                }
                u.describe(line.getOptionValue("d"));
            }
            else if (line.hasOption("q")) {
                u.query(line.getOptionValue("q"));
            }
            else if (line.hasOption("listStoredProcs")) {
                u.listStoredProcs();
            }
            else if (line.hasOption("s")) {
                u.dumpStoredProcedure(line.getOptionValue("s"));
            }
            else if (line.hasOption("v")) {
                u.dumpView(line.getOptionValue("v"));
            }
            else if (line.hasOption("input")) {
                u.processFile(line.getOptionValue("input"));
            }
            else if (line.hasOption("ddl")) {
                String s = line.getOptionValue("ddl");
                if (StringUtils.isNotEmpty(s)) {
                    String a[] = s.split(",");
                    if (a.length == 3) {
                        u.dumpDDL(a[0], a[1], a[2], "target/out");
                    }
                    else {
                        u.dumpDDL(a[0], "target/out");
                    }
                }
            }

        }
        catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
