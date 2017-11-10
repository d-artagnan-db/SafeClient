package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

public class MultiPut extends MultiOP {

	private final Put originalPut;
	private final List<Put> puts;

	public MultiPut(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, Put put) throws InvalidSecretValue, InvalidNumberOfBits, IOException {
		super(conf, connections, schema);
		this.originalPut = put;
		puts = generateMPCPut();
	}

	private List<Put> generateMPCPut() throws InvalidNumberOfBits, InvalidSecretValue, IOException {

	    byte[] row  = originalPut.getRow();
	    List<Put> putResults = new ArrayList<Put>();

	    for(int i=0; i < 4; i++){
	        putResults.add(new Put(row));
        }

        CellScanner scanner = originalPut.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();
            byte[] value = CellUtil.cloneValue(cell);
            byte[] bCF = CellUtil.cloneFamily(cell);
            byte[] bCQ = CellUtil.cloneQualifier(cell);
            String family = new String(bCF, Charset.forName("UTF-8"));
            String qualifier = new String(bCQ, Charset.forName("UTF-8"));
            List<byte[]> values = new ArrayList<byte[]>();

            if(schema.getCryptoTypeFromQualifier(family, qualifier) == DatabaseSchema.CryptoType.SMPC){

                int formatSize = schema.getFormatSizeFromQualifier(family, qualifier);

                Dealer dealer = new SharemindDealer(formatSize);
                BigInteger bigVal = new BigInteger(value);
                SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);

                values.add(secret.getU1().toByteArray());
                values.add(secret.getU2().toByteArray());
                values.add(secret.getU3().toByteArray());

            }else{

                values.add(value);
                values.add(value);
                values.add(value);
            }

            for(int i = 0; i < putResults.size(); i++){
                putResults.get(i).add(bCF, bCQ, values.get(i));
            }


        }
        return putResults;
    }
	@Override
	protected Thread queryThread(SharedClientConfiguration conf, HTable table,
			int index) {

		return new PutThread(config, table, puts.get(index));
	}

	@Override
	protected void threadsJoined(List<Thread> threads) throws IOException {
	}

}