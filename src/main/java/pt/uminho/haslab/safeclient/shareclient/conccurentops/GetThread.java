package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;

import java.io.IOException;

public class GetThread extends QueryThread {

    private Get originalGet;
    private boolean retryUntilVersionIsAvailable;

    public GetThread(SharedClientConfiguration conf, HTable table, Get get) {
        super(conf, table);
        this.originalGet = get;
        retryUntilVersionIsAvailable = false;
    }

    public GetThread(SharedClientConfiguration conf, HTable table, Get get, boolean retry) {
        super(conf, table);
        this.originalGet = get;
        retryUntilVersionIsAvailable = retry;
    }

    @Override
    protected void query() throws IOException {
        if (!retryUntilVersionIsAvailable) {
            res = table.get(originalGet);
        } else {
            try {
                //LOG.debug("Going to get missing value");
                int nretries = 0;
                int sleepTime = conf.getGetVersionSleep();
                do {
                    res = table.get(originalGet);
                    nretries += 1;
                    if (res.isEmpty()) {
                        Thread.sleep(sleepTime);
                        sleepTime += conf.getGetVersionSleep();
                    } else {
                        break;
                    }
                } while (nretries < conf.getGetVersionNRetries());
                //LOG.debug("Loop finished " + nretries + " Results " + res.isEmpty());
            } catch (InterruptedException e) {
                LOG.debug(e);
                throw new IllegalStateException(e);
            }
        }
    }
}
