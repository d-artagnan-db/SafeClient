package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;

import java.io.IOException;

public abstract class QueryThread implements Runnable {

    static final Log LOG = LogFactory.getLog(QueryThread.class.getName());

    protected final HTable table;

    protected Result res;

    protected SharedClientConfiguration conf;

    public QueryThread(SharedClientConfiguration conf, HTable table) {
        this.table = table;
        this.conf = conf;
    }

    public Result getResult() {
        return res;
    }

    protected abstract void query() throws IOException;

    @Override
    public void run() {
        try {
            query();
        } catch (IOException ex) {
            LOG.debug(ex);
            throw new IllegalStateException(ex);
        }

    }

}