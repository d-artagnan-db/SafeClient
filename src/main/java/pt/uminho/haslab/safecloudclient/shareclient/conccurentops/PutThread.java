package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

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
