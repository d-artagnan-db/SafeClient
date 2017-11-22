package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;

import java.io.IOException;
import java.util.List;

public class PutThread extends QueryThread {

    private Put put;

    private List<Put> batchPut;

    private boolean isBatchPut;

	public PutThread(SharedClientConfiguration config, HTable table, Put put) {
		super(config, table);
		this.put = put;
	}

    public PutThread(SharedClientConfiguration config, HTable table, List<Put> puts) {
        super(config, table);
        this.batchPut = puts;
        isBatchPut = true;
    }

    @Override
    protected void query() throws IOException {
        if (!isBatchPut) {
            table.put(put);
        } else {
            table.put(batchPut);
        }
    }

}
