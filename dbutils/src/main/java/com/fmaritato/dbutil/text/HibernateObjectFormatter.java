package com.fmaritato.dbutil.text;
/**
 * This class will auto generate a hibernate value object class for a given table.
 *
 * @author maritatf
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;

public class HibernateObjectFormatter implements Formatter {

    private transient Log log = LogFactory.getLog(HibernateObjectFormatter.class);

    public void format(ResultSetMetaData meta, Writer out) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void format(ResultSet rs, Writer out) throws Exception {
        format(rs, out, true);
    }

    /**
     * We don't care about the data in this class. Just the table meta data.
     *
     * @param rs
     * @param out
     * @throws Exception
     */
    public void format(ResultSet rs, Writer out, boolean writeHeader) throws Exception {
        StringBuffer buf = new StringBuffer(1024);
        printHeader(buf);

        HashMap<String, String> m = new HashMap<String, String>();
        String tableName = null;
        while (rs.next()) {

            if (tableName == null) {
                tableName = rs.getString("TABLE_NAME");
                printTableHeader(tableName, buf);
            }

            String colName = rs.getString("COLUMN_NAME");
            int colType = rs.getInt("DATA_TYPE");
            String ct = convertColumnType(colType);
            String cn = convertColumnName(colName);
            buf.append("  private ").append(ct).append(" ").append(cn).append(";\n");
            m.put(colName, ct);
        }
        buf.append("\n");

        for (String name : m.keySet()) {
            String cn = convertColumnName(name);
            buf.append("  @Column(name=\"").append(name).append("\")\n")
                    .append("  public ")
                    .append(m.get(name))
                    .append(" get")
                    .append(cn.substring(0, 1).toUpperCase())
                    .append(cn.substring(1, cn.length()))
                    .append("() { return ")
                    .append(cn)
                    .append("; }\n\n")
                    .append("  public void ")
                    .append("set")
                    .append(cn.substring(0, 1).toUpperCase())
                    .append(cn.substring(1, cn.length()))
                    .append("(")
                    .append(m.get(name))
                    .append(" ")
                    .append(cn)
                    .append(") { this.")
                    .append(cn)
                    .append(" = ")
                    .append(cn)
                    .append("; }\n\n");
        }
        buf.append("}\n");

        out.write(buf.toString());
        out.flush();

    }

    private String convertColumnName(String name) {
        return toCamelCase(name, false);
    }

    private String convertColumnType(int columnType) {
        log.debug("TYPE: " + columnType);
        switch (columnType) {
            case Types.CHAR:
                return "Char";
            case Types.DECIMAL:
                return "Long";
            case Types.NUMERIC:
                return "Long";
            case Types.FLOAT:
                return "Double";
            case Types.TIMESTAMP:
            case Types.DATE:
                return "Date";
            default:
                return "String";
        }
    }

    private void printTableHeader(String tableName, StringBuffer buf) {
        String ccTableName = toCamelCase(tableName, true);
        log.debug("table name: " + tableName);
        log.debug("cctable name: " + ccTableName);
        buf.append("@Table(name=\"").append(tableName).append("\")\n");
        buf.append("public class ").append(ccTableName).append(" implements Serializable {\n");
    }

    private void printHeader(StringBuffer buf) throws IOException {
        buf.append("import javax.persistence.Column;\n");
        buf.append("import javax.persistence.Entity;\n");
        buf.append("import javax.persistence.Id;\n");
        buf.append("import javax.persistence.Table;\n");
        buf.append("import java.io.Serializable;\n\n");
        buf.append("import java.util.Date;\n\n");
        buf.append("@Entity\n");
    }

    private String toCamelCase(String s, boolean className) {
        StringBuffer tmp = new StringBuffer();
        int last = 0;
        int next;
        // make sure first letter is capital if we are making a class name.
        if (className) {
            tmp.append(s.substring(0,1).toUpperCase());
            last = 1;
        }
        while ((next = s.indexOf("_", last)) > 0) {
            String ss = s.toLowerCase().substring(last, next);
            String s2 = s.toLowerCase().substring(next + 1, next + 2);
            tmp.append(ss).append(s2.toUpperCase());
            last = next + 2;
        }
        tmp.append(s.toLowerCase().substring(last, s.length()));
        return tmp.toString();
    }
    
    public void setTableName(String tableName) {
        ; //do nothing
    }
}
