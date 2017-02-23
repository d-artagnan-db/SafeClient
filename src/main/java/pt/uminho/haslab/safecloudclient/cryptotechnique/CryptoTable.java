package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by rgmacedo on 2/20/17.
 */
public class CryptoTable extends HTable {

    public CryptoProperties cryptoProperties;
    public ResultScannerFactory resultScannerFactory;


    public CryptoTable(CryptoTechnique.CryptoType cType) {
        this.cryptoProperties = new CryptoProperties(cType);
        this.resultScannerFactory = new ResultScannerFactory();
    }

    public CryptoTable(Configuration conf, String tableName, CryptoTechnique.CryptoType cType) throws IOException {
        super(conf, TableName.valueOf(tableName));
        this.cryptoProperties = new CryptoProperties(cType);
        this.resultScannerFactory = new ResultScannerFactory();
    }

    public byte[] addPadding(byte[] value) {
        String s = new BigInteger(value).toString();
        System.out.println("Before: "+s);
        s = String.format("%023d", new BigInteger(value));
        System.out.println("After: "+s);
        System.out.println("Going to insert value " + s);
        return s.getBytes();
    }


    @Override
    public void put(Put put) {
        try {
//            byte[] row = put.getRow();
            byte[] row = addPadding(put.getRow());
            Put encPut = new Put(this.cryptoProperties.encode(row));
            CellScanner cs = put.cellScanner();

            while (cs.advance()) {
                Cell cell = cs.current();
                encPut.add(CellUtil.cloneFamily(cell),
                        CellUtil.cloneQualifier(cell),
                        this.cryptoProperties.encode(CellUtil.cloneValue(cell)));
            }
            super.put(encPut);
//            System.out.println("Put: "+this.cType+" - "+encPut.toString());
        } catch (IOException e) {
            System.out.println("CryptoTable: Exception in put method - "+e.getMessage());
        }
    }

    /*
    TODO
    (4) Otimizar, não fazer para todos os elementos
     */
    @Override
    public Result get(Get get) {
        Scan getScan = new Scan();
        Result getResult = null;
        try {
            switch(this.cryptoProperties.cType) {
                case STD:
//                      TODO otimizar isto para não fazer get a tudo
                    BigInteger wantedValue = new BigInteger(get.getRow());
                    ResultScanner encScan = super.getScanner(getScan);
                    for (Result r = encScan.next(); r != null; r = encScan.next()) {
                        Result res = this.cryptoProperties.decodeResult(r.getRow(), r);
                        BigInteger aux = new BigInteger(res.getRow());
                        if (wantedValue.equals(aux)) {
                            getResult = res;
                            break;
                        }
                    }
                    return getResult;
                case DET:
                case OPE:
                    getResult = null;
//                    TODO para o OPE inicializar dinamicamente os tamanhos do plaintext e ciphertext
                    byte[] row = get.getRow();
                    Get encGet = new Get(this.cryptoProperties.encode(row));
                    Result res = super.get(encGet);
                    if(!res.isEmpty()) {
                        getResult = this.cryptoProperties.decodeResult(res.getRow(), res);
                    }
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
    (4) Otimizar, não fazer para todos os elementos
     */
    @Override
    public void delete(Delete delete) {
        Scan deleteScan = new Scan();
        try {
            switch(this.cryptoProperties.cType) {
                case STD:
                    BigInteger wantedValue = new BigInteger(delete.getRow());
                    ResultScanner encScan = super.getScanner(deleteScan);
                    for(Result r = encScan.next(); r != null; r = encScan.next()) {
                        BigInteger resultValue = new BigInteger(this.cryptoProperties.decodeResult(r.getRow(), r).getRow());
                        if(wantedValue.equals(resultValue)) {
                            System.out.println("Row deleted: "+resultValue);
                            Delete del = new Delete(r.getRow());
                            super.delete(del);
                        }
                    }
                    break;
                case DET:
                case OPE:
                    byte[] row = delete.getRow();
                    Delete encDelete = new Delete(this.cryptoProperties.encode(row));
                    super.delete(encDelete);
                    System.out.println("Row deleted: "+new BigInteger(row));
                    break;
                default:
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        byte[] startRow = scan.getStartRow();
        byte[] endRow = scan.getStopRow();

        Scan encScan = this.cryptoProperties.encryptedScan(scan);
        ResultScanner encryptedResultScanner = super.getScanner(encScan);

        return this.resultScannerFactory.getResultScanner(
                        this.cryptoProperties.cType,
                        this.cryptoProperties,
                        startRow,
                        endRow,
                        encryptedResultScanner,
                        this.cryptoProperties.parseFilter((RowFilter) scan.getFilter()));

    }

}
