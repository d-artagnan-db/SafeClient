package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;

import java.io.IOException;

public class PutThread extends QueryThread {

	private final Put put;

	public PutThread(SharedClientConfiguration config, HTable table, Put put) {
		super(config, table);
		this.put = put;
	}

	@Override
	protected void query() throws IOException {
		table.put(put);
	}

}
