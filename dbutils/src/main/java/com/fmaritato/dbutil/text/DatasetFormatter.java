package com.fmaritato.dbutil.text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.LinkedList;
import java.util.List;

/**
 * User: fmaritato
 * Date: Jul 17, 2007
 */
public class DatasetFormatter implements Formatter {

    private transient Log log = LogFactory.getLog(DatasetFormatter.class);

    private String tableName = null;
    private List<String> columns = new LinkedList<String>();

    public void format(ResultSet rs, Writer out) throws Exception {
        format(rs, out, true);
    }

    public void format(ResultSetMetaData meta, Writer out) throws Exception {
        int count = meta.getColumnCount();
        for (int i = 1; i < count + 1; i++) {
            String n = meta.getColumnName(i);
            if (log.isInfoEnabled()) {
                log.info("column: " + n);
            }
            columns.add(n);
        }
    }

    public void format(ResultSet rs, Writer out, boolean writeHeader) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        format(meta, out);

        out.write("<dataset>");
        while (rs.next()) {
            out.write("<");
            if (tableName != null) {
                if (log.isWarnEnabled()) {
                    log.warn("no table name set...try -t option");
                }
                out.write(tableName);
            }
            out.write("\n");
            for (String column : columns) {
                out.write(column);
                out.write("=\"");
                Object v = rs.getObject(column);
                if (v != null) {
                    String value;
                    if (v instanceof oracle.sql.TIMESTAMP) {
                        value = ((oracle.sql.TIMESTAMP) v).timestampValue().toString();
                    }
                    else {
                        value = v.toString();
                    }
                    value = value.replace("<", "&lt;")
                            .replace(">", "&gt;")
                            .replace("\"", "&quot;")
                            .replace("\'", "&apos;");
                    out.write(value);
                }
                out.write("\"");
                out.write("\n");
                out.flush();
            }
            out.write("/>\n");
            out.flush();
        }
        out.write("</dataset>");
        out.flush();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
