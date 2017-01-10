package pt.uminho.haslab.safecloudclient.opeclient;

import java.io.InterruptedIOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

public class OPEHTable extends HTable {

	@Override
	public void put(final Put put) throws InterruptedIOException,
			RetriesExhaustedWithDetailsException {
		// Super secret ope
		byte[] row = put.getRow();
		// magic ope encription
		// Put valueCipherd = new Put(encruptiod)
		// super.put(valueCiphered);
	}
}
