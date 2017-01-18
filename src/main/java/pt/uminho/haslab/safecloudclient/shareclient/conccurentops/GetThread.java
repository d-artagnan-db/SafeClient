package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class GetThread extends QueryThread {

	private final byte[] secretRow;
	public GetThread(SharedClientConfiguration conf, HTable table,
			byte[] secretRow, long requestID, int targetPlayer) {
		super(conf, table, requestID, targetPlayer);
		this.secretRow = secretRow;
	}

	@Override
	protected void query() throws IOException {
		Get get = new Get(secretRow);
		get.setAttribute("requestID", ("" + requestID).getBytes());
		get.setAttribute("targetPlayer", ("" + targetPlayer).getBytes());
		res = table.get(get);
	}
}
