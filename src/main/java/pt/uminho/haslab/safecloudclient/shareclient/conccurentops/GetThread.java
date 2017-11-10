package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class GetThread extends QueryThread {

	private Get originalGet;

	public GetThread(SharedClientConfiguration conf, HTable table, Get get){
		super(conf, table);
		this.originalGet = get;
	}

	@Override
	protected void query() throws IOException {
		res = table.get(originalGet);
	}
}
