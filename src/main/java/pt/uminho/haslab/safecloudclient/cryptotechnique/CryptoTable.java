package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 2/20/17.
 */
public class CryptoTable extends HTable {

    public CryptoTechnique.CryptoType cType;
    public CryptoHandler handler;
    public byte[] key;
    public Utils utils;

    public CryptoTable(CryptoTechnique.CryptoType cType) {
        this.cType = cType;
        this.handler = new CryptoHandler();
        this.key = this.handler.gen(cType);
        this.utils = new Utils();
    }


    public CryptoTable(Configuration conf, String tableName, CryptoTechnique.CryptoType cType) throws IOException {
        super(conf, TableName.valueOf(tableName));
        this.cType = cType;
        this.handler = new CryptoHandler();
        this.key = this.handler.gen(cType);
        this.utils = new Utils();
    }


    public byte[] encode(byte[] content) {
        return this.handler.encrypt(this.cType, this.key, content);
    }

    public byte[] decode(byte[] content) {
        return this.handler.decrypt(this.cType, this.key, content);
    }

    @Override
    public void put(Put put) {
        try {
            byte[] row = put.getRow();
            Put encPut = new Put(this.encode(row));
            CellScanner cs = put.cellScanner();

            while (cs.advance()) {
                Cell cell = cs.current();
                encPut.add(CellUtil.cloneFamily(cell),
                        CellUtil.cloneQualifier(cell),
                        this.encode(CellUtil.cloneValue(cell)));
            }
            super.put(encPut);
            System.out.println("Put: "+encPut.toString());
        } catch (IOException e) {
            System.out.println("CryptoTable: Exception in put method - "+e.getMessage());
        }
    }


    private Result decodeResult(byte[] row, Result res) {
        List<Cell> cellList = new ArrayList<Cell>();
        while (res.advance()) {
            Cell cell = res.current();
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] cq = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            long timestamp = cell.getTimestamp();
            byte type = cell.getTypeByte();

            Cell decCell = CellUtil.createCell(
                    this.decode(row),
                    cf,
                    cq,
                    timestamp,
                    type,
                    this.decode(value));
            cellList.add(decCell);

        }
        return Result.create(cellList);
    }

    /*
    TODO
    (1) Fazer get de todos os valores - done
    (2) Decifrar todos - done
    (3) Comparar com o valor que se pretende e devolver - done
    (4) Otimizar, não fazer para todos os elementos
     */
    @Override
    public Result get(Get get) {
        this.utils = new Utils();
        Scan newScan = new Scan();
        Result getResult = null;
        try {
            switch(this.cType) {
                case STD:
//                    TODO otimizar isto para não fazer get a tudo
                    BigInteger wantedValue = new BigInteger(get.getRow());
                    ResultScanner encScan = super.getScanner(newScan);
                    for (Result r = encScan.next(); r != null; r = encScan.next()) {
                        Result res = this.decodeResult(r.getRow(), r);
                        BigInteger aux = new BigInteger(res.getRow());
                        if (wantedValue.equals(aux)) {
                            getResult = res;
                            break;
                        }
                    }
                case DET:
                case OPE:
//                    TODO para o OPE inicializar dinamicamente os tamanhos do plaintext e ciphertext
                    byte[] row = get.getRow();
                    Get encGet = new Get(this.encode(row));
                    Result res = super.get(encGet);
                    System.out.println("Res: "+res.toString());
                    getResult = decodeResult(res.getRow(), res);
                    return getResult;
                default:
                    break;
            }

        } catch (IOException e) {
            System.out.println("CryptoTable: Exception in get. "+e.getMessage());
        }
        return getResult;
    }

    /*
    TODO
    (1) Fazer get de todos os valores
    (2) Decifrar todos
    (3) Comparar com o valor que se pretende e devolver
    (4) Otimizar, não fazer para todos os elementos
     */
    @Override
    public void delete(Delete delete) {

    }

    public Result scan(Scan scan) {
        return null;
    }

}
