package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class MultiScan extends MultiOP implements ResultScanner {

	static final Log LOG = LogFactory.getLog(MultiScan.class.getName());

	private final List<byte[]> startRow;
	private final List<byte[]> stopRow;
	private final List<Thread> scans;

	public MultiScan(SharedClientConfiguration config,
			List<HTable> connections, long requestID, int targetPlayer,
			List<byte[]> startRow, List<byte[]> stopRow) {
		super(config, connections, requestID, targetPlayer);

		this.startRow = startRow;
		this.stopRow = stopRow;
		scans = new ArrayList<Thread>();
	}

	@Override
	protected Thread queryThread(SharedClientConfiguration config,
			HTable table, int index) {
		try {
			LOG.debug(startRow.isEmpty());
			LOG.debug(stopRow.isEmpty());
			if (!startRow.isEmpty() && !stopRow.isEmpty()) {
				LOG.debug("1");
				Thread t = new ResultScannerThread(config, table, requestID,
						targetPlayer, startRow.get(index), stopRow.get(index));
				scans.add(t);
				return t;
			} else if (!startRow.isEmpty() && stopRow.isEmpty()) {
				LOG.debug("2");
				Thread t = new ResultScannerThread(config, table, requestID,
						targetPlayer, startRow.get(index), new byte[0]);
				LOG.debug("Thread is null? " + t);
				scans.add(t);
				return t;
			} else if (startRow.isEmpty() && !stopRow.isEmpty()) {
				LOG.debug("3");
				Thread t = new ResultScannerThread(config, table, requestID,
						targetPlayer, new byte[0], stopRow.get(index));
				scans.add(t);
				return t;
			} else if (startRow.isEmpty() && stopRow.isEmpty()) {
				LOG.debug(4);
				Thread t = new ResultScannerThread(config, table, requestID,
						targetPlayer, new byte[0], new byte[0]);
				scans.add(t);
				return t;

			}

		} catch (IOException ex) {
			LOG.error(ex.getMessage());
			throw new IllegalStateException(ex);
		}
		return null;
	}

	@Override
	protected void threadsJoined(List<Thread> threads) throws IOException {
	}

	public Result next() throws IOException {
		List<Result> results = new ArrayList<Result>();
		for (Thread t : scans) {
			Result rst = ((ResultScannerThread) t).next();
			results.add(rst);
		}
		if (results.get(0).isEmpty()) {
			return null;
		}
		return decodeResult(results);
	}

	public Result[] next(int i) throws IOException {
		throw new UnsupportedOperationException("Not supported yet."); // Templates.
	}

	public void close() {
		for (Thread t : scans) {
			((ResultScannerThread) t).close();
		}
	}

	public Iterator<Result> iterator() {
		throw new UnsupportedOperationException("Not supported yet."); // Templates.
	}

}
