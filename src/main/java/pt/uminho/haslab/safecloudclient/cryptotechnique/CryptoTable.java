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
        this.cryptoProperties = new CryptoProperties(cType, 23);
        this.resultScannerFactory = new ResultScannerFactory();
    }

    public CryptoTable(Configuration conf, String tableName, CryptoTechnique.CryptoType cType) throws IOException {
        super(conf, TableName.valueOf(tableName));
        this.cryptoProperties = new CryptoProperties(cType, 23);
        this.resultScannerFactory = new ResultScannerFactory();
    }


    @Override
    public void put(Put put) {
        try {
            byte[] row = this.cryptoProperties.addPadding(put.getRow());
            Put encPut = new Put(this.cryptoProperties.encode(row));
            CellScanner cs = put.cellScanner();

            while (cs.advance()) {
                Cell cell = cs.current();
                encPut.add(CellUtil.cloneFamily(cell),
                        CellUtil.cloneQualifier(cell),
                        this.cryptoProperties.encode(CellUtil.cloneValue(cell)));
            }
            super.put(encPut);

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
            byte[] row = this.cryptoProperties.addPadding(get.getRow());
            switch(this.cryptoProperties.cType) {
                case STD:
                    BigInteger wantedValue = new BigInteger(row);
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
            byte[] row = this.cryptoProperties.addPadding(delete.getRow());
            switch(this.cryptoProperties.cType) {
                case STD:
                    BigInteger wantedValue = new BigInteger(row);
                    ResultScanner encScan = super.getScanner(deleteScan);
                    for(Result r = encScan.next(); r != null; r = encScan.next()) {
                        BigInteger resultValue = new BigInteger(this.cryptoProperties.decodeResult(r.getRow(), r).getRow());
                        if(wantedValue.equals(resultValue)) {
                            Delete del = new Delete(r.getRow());
                            super.delete(del);
                            System.out.println("Row deleted: "+new String(row));
                        }
                    }
                    break;
                case DET:
                case OPE:
                    Delete encDelete = new Delete(this.cryptoProperties.encode(row));
                    super.delete(encDelete);
                    System.out.println("Row deleted: "+new String(row));
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
        byte[] startRow = this.cryptoProperties.addPadding(scan.getStartRow());
        byte[] endRow = this.cryptoProperties.addPadding(scan.getStopRow());

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
