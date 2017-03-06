package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public abstract class QueryThread extends Thread {
	static final Log LOG = LogFactory.getLog(QueryThread.class.getName());

	protected final HTable table;

	protected Result res;

	protected final long requestID;

	protected final int targetPlayer;

	protected SharedClientConfiguration conf;

	public QueryThread(SharedClientConfiguration conf, HTable table,
			long requestID, int targetPlayer) {
		this.table = table;
		this.requestID = requestID;
		this.targetPlayer = targetPlayer;
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