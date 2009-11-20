package com.fmaritato.dbutil.text;

import java.io.Writer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public interface Formatter {

    public void format(ResultSet rs, Writer out, boolean writeHeader) throws Exception;

    public void format(ResultSet rs, Writer out) throws Exception;

    public void format(ResultSetMetaData meta, Writer out) throws Exception;

}
