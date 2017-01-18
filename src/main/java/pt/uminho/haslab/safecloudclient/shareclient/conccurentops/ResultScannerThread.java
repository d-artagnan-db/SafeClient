package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class ResultScannerThread extends QueryThread implements ResultScanner {

	private final ResultScanner resultScanner;
	private final LinkedBlockingDeque<Result> results;

	public ResultScannerThread(SharedClientConfiguration config, HTable table,
			long requestID, int targetPlayer, byte[] startRow, byte[] stopRow)
			throws IOException {
		super(config, table, requestID, targetPlayer);
		Scan scan = null;
		if (startRow.length != 0 && stopRow.length != 0) {
			scan = new Scan(startRow, stopRow);
		} else if (startRow.length != 0 && stopRow.length == 0) {
			scan = new Scan(startRow);
		} else if (startRow.length == 0 && stopRow.length == 0) {
			scan = new Scan();
		}
		scan.setAttribute("requestID", ("" + requestID).getBytes());
		scan.setAttribute("targetPlayer", ("" + targetPlayer).getBytes());
		resultScanner = table.getScanner(scan);
		results = new LinkedBlockingDeque<Result>();
	}

	public Result next() throws IOException {
		try {
			// System.out.println("going to return value on next "+
			// results.size());
			return results.take();
			// return Result.EMPTY_RESULT;
		} catch (InterruptedException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public Result[] next(int i) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void close() {
		// resultScanner.close();
	}

	public Iterator<Result> iterator() {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected void query() throws IOException {
		for (Result result = resultScanner.next(); result != null; result = resultScanner
				.next()) {
			results.add(result);
		}
		results.add(Result.EMPTY_RESULT);

	}

}
