package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

public class ResultScannerThread extends QueryThread implements ResultScanner {

	private final ResultScanner resultScanner;
	private final LinkedBlockingDeque<Result> results;

    public ResultScannerThread(SharedClientConfiguration config, HTable table, Scan scan)
            throws IOException {
        super(config, table);
        results = new LinkedBlockingDeque<Result>();
        resultScanner = table.getScanner(scan);

    }

	public Result next() throws IOException {
		try {
			return results.take();
		} catch (InterruptedException ex) {
			throw new IllegalStateException(ex);
		}
	}

	public Result[] next(int i) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void close() {
		resultScanner.close();
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
