package com.fmaritato.dbutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;

/**
 * User: fmaritato
 * Date: May 29, 2007
 */
public class SybaseDBUtil extends DBUtil {

    private transient Log log = LogFactory.getLog(SybaseDBUtil.class);

    public void dumpDDL(String schema, String obj, String type, String dir) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void dumpDDL(String schema, String dir) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void listStoredProcs() {
        query("select name from sysobjects where type='P' order by name");
    }

    public void dumpStoredProcedure(String procName) {
        HashMap m = new HashMap();
        m.put("1", procName);
        setBindVariables(m);
        query("select text from sysobjects a, syscomments b where a.id=b.id and a.name=?");
    }

    public void dumpView(String viewName) {
        dumpStoredProcedure(viewName);
    }
}
