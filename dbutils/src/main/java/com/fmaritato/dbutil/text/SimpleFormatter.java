package com.fmaritato.dbutil.text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Reader;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class SimpleFormatter implements Formatter {

    private static Log logger =
            LogFactory.getLog(SimpleFormatter.class);

    public static final String PROP_DELIM = "SimpleFormatter.delimiter";

    private String delim = " "; // space by default

    public String getDelim() {
        return delim;
    }

    public void setDelim(String aValue) {
        delim = aValue;
    }

    public SimpleFormatter() {
        this(System.getProperty(PROP_DELIM));
    }

    public SimpleFormatter(String delim) {
        setDelim(delim);
    }

    public void format(ResultSet rs, Writer out) throws Exception {
        format(rs, out, true);
    }

    public void format(ResultSet rs, Writer out, boolean writeHeader) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        if (writeHeader) {
            format(meta, out);
        }
        int numberOfColumns = meta.getColumnCount();
        logger.debug("number of columns: " + numberOfColumns);

        StringBuffer buf = new StringBuffer(256);
        while (rs.next()) {
            buf.delete(0, buf.length());
            for (int i = 1; i < numberOfColumns + 1; i++) {
                Object o = rs.getObject(i);

                if (o instanceof oracle.sql.CLOB) {
                    Reader r = ((oracle.sql.CLOB) o).getCharacterStream();
                    int c;
                    while ((c = r.read()) > 0) {
                        buf.append((char) c);
                    }
                    r.close();
                }
                else if (o instanceof oracle.sql.TIMESTAMP) {
                    buf.append(((oracle.sql.TIMESTAMP) o).timestampValue().toString());
                }
                else {
                    buf.append(o);
                }

                if ((i + 1) <= numberOfColumns) {
                    buf.append(delim);
                }
            }
            buf.append("\n");
            out.write(buf.toString());
            out.flush();
        }
    }

    public void format(ResultSetMetaData meta, Writer out) throws Exception {
        StringBuffer buf = new StringBuffer(256);
        int numberOfColumns = meta.getColumnCount();
        for (int i = 1; i < numberOfColumns + 1; i++) {
            buf.append(meta.getColumnName(i));
            if ((i + 1) <= numberOfColumns) {
                buf.append(delim);
            }
        }
        buf.append("\n");
        out.write(buf.toString());
        out.flush();
    }

    public void setTableName(String tableName) {
        ; //do nothing
    }
}
