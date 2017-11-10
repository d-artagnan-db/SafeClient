package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.TableSchema;

public class MultiGet extends MultiOP {

	private Result result;
	private Get originalGet;

	public MultiGet(SharedClientConfiguration config, List<HTable> connections, TableSchema schema, Get get) {
		super(config, connections, schema);
		result = Result.EMPTY_RESULT;
		this.originalGet = get;
	}

	@Override
	protected Thread queryThread(SharedClientConfiguration config,
			HTable table, int index) {
		return new GetThread(config, table, originalGet);
	}

	public Result getResult() {
		return result;
	}

	@Override
	protected void threadsJoined(List<Thread> threads) throws IOException {

		Result resOne = ((QueryThread) threads.get(0)).getResult();
		Result resTwo = ((QueryThread) threads.get(1)).getResult();
		Result resThree = ((QueryThread) threads.get(2)).getResult();

		List<Result> results = new ArrayList<Result>();
		results.add(resOne);
		results.add(resTwo);
		results.add(resThree);

		if (results.get(0).isEmpty()) {
			LOG.debug("Multi Get returned empty result");
			result = Result.EMPTY_RESULT;
		} else {
			LOG.debug("Multi Get found result");
			result = decodeResult(results);
		}

	}

}