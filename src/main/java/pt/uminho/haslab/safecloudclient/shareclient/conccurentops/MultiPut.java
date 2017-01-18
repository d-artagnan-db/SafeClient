package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class MultiPut extends MultiOP {

	private final byte[] maxKey;
	private final Put originalPut;
	private final List<byte[]> secrets;

	public MultiPut(SharedClientConfiguration conf, List<HTable> connections,
			List<byte[]> secrets, long requestID, int targetPlayer, Put put,
			byte[] maxKey) {
		super(conf, connections, requestID, targetPlayer);
		this.maxKey = maxKey;
		this.originalPut = put;
		this.secrets = secrets;
	}

	@Override
	protected Thread queryThread(SharedClientConfiguration conf, HTable table,
			int rowid) {
		return new PutThread(config, table, secrets.get(rowid), requestID,
				targetPlayer, originalPut, maxKey);
	}

	@Override
	protected void threadsJoined(List<Thread> threads) throws IOException {
	}

}