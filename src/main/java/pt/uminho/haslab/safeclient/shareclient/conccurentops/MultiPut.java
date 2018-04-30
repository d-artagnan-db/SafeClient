package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiPut extends MultiOP {

    // SinglePut variables
    private List<Put> protectedPuts;

    // BatchPutOperations
    private List<List<Put>> protectedBatchPuts;

    private boolean isBatchPut;

    public MultiPut(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, Put put) throws InvalidSecretValue, InvalidNumberOfBits, IOException {
        super(conf, connections, schema);
        protectedPuts = generateMPCPut(put);
    }

    public MultiPut(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, List<Put> puts) throws InvalidSecretValue, InvalidNumberOfBits, IOException {
        super(conf, connections, schema);
        protectedBatchPuts = generateBatchMPCPut(puts);
        isBatchPut = true;
    }

    private List<List<Put>> generateBatchMPCPut(List<Put> originalBatchPut) throws InvalidNumberOfBits, IOException, InvalidSecretValue {
        List<List<Put>> protectedPuts = new ArrayList<List<Put>>();

        for (Put p : originalBatchPut) {
            protectedPuts.add(generateMPCPut(p));
        }

        List<List<Put>> resultingPuts = new ArrayList<List<Put>>();
        resultingPuts.add(new ArrayList<Put>());
        resultingPuts.add(new ArrayList<Put>());
        resultingPuts.add(new ArrayList<Put>());

        for (List<Put> p : protectedPuts) {
            for (int i = 0; i < 3; i++) {
                resultingPuts.get(i).add(p.get(i));
            }
        }

        return resultingPuts;
    }

    @Override
    protected Thread queryThread(SharedClientConfiguration conf, HTable table,
                                 int index) {

        if (!isBatchPut) {
            return new PutThread(config, table, protectedPuts.get(index));
        } else {
            return new PutThread(config, table, protectedBatchPuts.get(index));

        }
    }

    @Override
    protected void threadsJoined(List<Thread> threads) throws IOException {
    }

}