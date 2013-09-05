package com.fmaritato.dbutil.text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Reader;
import java.io.Writer;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class TabularFormatter implements Formatter {

    private final static Log log =
            LogFactory.getLog(TabularFormatter.class);

    private String delim = "|";

    /**
     * Add a maximum column size to avoid OOME's
     */
    private static final int maxColumnPrecision = 1000;

    public TabularFormatter() {
    }

    @Override
    public void format(DatabaseMetaData meta, String schema, String table, Writer out) throws Exception {
        throw new RuntimeException("Not implemented");
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

        int rowCount = 0;
        StringBuffer buf = new StringBuffer(256);
        while (rs.next()) {
            buf.delete(0, buf.length());
            for (int i = 1; i < numberOfColumns + 1; i++) {
                int precision = Math.max(meta.getPrecision(i),
                                         meta.getColumnName(i).length() + 1);
                precision = Math.min(maxColumnPrecision,
                                     precision);
                if (i == 1) {
                    buf.append(delim).append(" ");
                }
                Object o = rs.getObject(i);
                if (o instanceof oracle.sql.TIMESTAMP) {
                    buf.append(((oracle.sql.TIMESTAMP) o).timestampValue());
                }
                else if (o instanceof oracle.sql.CLOB) {
                    Reader r = ((oracle.sql.CLOB) o).getCharacterStream();
                    int c;
                    while ((c = r.read()) > 0) {
                        buf.append((char) c);
                    }
                    r.close();
                }
                else {

                    buf.append(o);
                }
                if ((i + 1) <= numberOfColumns) {
                    for (int j = ("" + o).length(); j < precision; j++) {
                        buf.append(" ");
                    }
                    buf.append(delim).append(" ");
                }
                else {
                    for (int j = ("" + o).length(); j < precision; j++) {
                        buf.append(" ");
                    }
                    buf.append(delim);
                }
            }
            buf.append("\n");
            out.write(buf.toString());
            out.flush();
            rowCount++;
        }
        writeSeparator(out, buf.length());
        out.flush();

        buf.delete(0, buf.length());
        buf.append(rowCount).append(" matching rows\n");
        out.write(buf.toString());
        out.flush();
    }

    public void format(ResultSetMetaData meta, Writer out) throws Exception {
        StringBuffer buf = new StringBuffer(256);
        int numberOfColumns = meta.getColumnCount();

        for (int i = 1; i < numberOfColumns + 1; i++) {
            int precision = Math.max(meta.getPrecision(i),
                                     meta.getColumnName(i).length() + 1);
            precision = Math.min(maxColumnPrecision,
                                 precision);
            if (i == 1) {
                buf.append(delim).append(" ");
            }
            buf.append(meta.getColumnName(i));
            if ((i + 1) <= numberOfColumns) {
                for (int j = meta.getColumnName(i).length(); j < precision; j++) {
                    buf.append(" ");
                }
                buf.append(delim).append(" ");
            }
            else {
                for (int j = meta.getColumnName(i).length(); j < precision; j++) {
                    buf.append(" ");
                }
                buf.append(delim);
            }
        }
        buf.append("\n");
        writeSeparator(out, buf.length());
        out.write(buf.toString());
        writeSeparator(out, buf.length());
        out.flush();
    }

    public void writeSeparator(Writer out, int length) throws Exception {
        for (int i = 0; i < length - 1; i++) {
            out.write("-");
        }
        out.write("\n");
    }
    public void setTableName(String tableName) {
        ; //do nothing
    }
}
