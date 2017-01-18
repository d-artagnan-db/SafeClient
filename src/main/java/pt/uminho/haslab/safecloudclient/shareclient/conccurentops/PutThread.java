package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;

public class PutThread extends QueryThread {

	private final byte[] maxKey;
	private final Put put;
	protected final byte[] secretRow;

	public PutThread(SharedClientConfiguration config, HTable table,
			byte[] secretRow, long requestID, int targetPlayer, Put put,
			byte[] maxKey) {
		super(config, table, requestID, targetPlayer);
		this.secretRow = secretRow;
		this.maxKey = maxKey;
		this.put = put;
	}

	@Override
	protected void query() throws IOException {
		Put secretPut = createSecretRegionPut(put, maxKey, secretRow);
		secretPut.setAttribute("requestID", ("" + requestID).getBytes());
		secretPut.setAttribute("targetPlayer", ("" + targetPlayer).getBytes());
		table.put(secretPut);
	}

	private Put createSecretRegionPut(Put put, byte[] identifier,
			byte[] virtualKey) throws IOException {
		Put secretPut = new Put(identifier);
		CellScanner cs = put.cellScanner();

		while (cs.advance()) {
			Cell cell = cs.current();

			secretPut.add(CellUtil.cloneFamily(cell),
					CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
		}

		secretPut.add(conf.getShareKeyColumnFamily().getBytes(), conf
				.getShareKeyColumnQualifier().getBytes(), virtualKey);
		return secretPut;

	}
}
