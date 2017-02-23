package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 2/22/17.
 */
public class CryptoProperties {

    public CryptoHandler handler;
    public CryptoTechnique.CryptoType cType;
    public byte[] key;
    public Utils utils;

    public CryptoProperties(CryptoTechnique.CryptoType cType) {
        this.handler = new CryptoHandler();
        this.cType = cType;
        this.key = this.handler.gen(cType);
        this.utils = new Utils();
    }

    public byte[] encode(byte[] content) {
        return this.handler.encrypt(this.cType, this.key, content);
    }

    public byte[] decode(byte[] content) {
        return this.handler.decrypt(this.cType, this.key, content);
    }

    public Result decodeResult(byte[] row, Result res) {
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

    public Scan encryptedScan(Scan s) {
        byte[] startRow = s.getStartRow();
        byte[] stopRow = s.getStopRow();
        Scan encScan = null;

        switch(this.cType) {
            case STD:
            case DET:
                encScan = new Scan();
                break;
            case OPE:
                encScan = new Scan();
                if (startRow.length != 0 && stopRow.length != 0) {
                    encScan.setStartRow(this.encode(startRow));
                    encScan.setStopRow(this.encode(stopRow));
                } else if (startRow.length != 0 && stopRow.length == 0) {
                    encScan.setStartRow(this.encode(startRow));
                } else if (startRow.length == 0 && stopRow.length != 0) {
                    encScan.setStopRow(this.encode(stopRow));
                }

                if (s.hasFilter()) {
                    RowFilter encryptedFilter = (RowFilter) parseFilter((RowFilter) s.getFilter());
                    encScan.setFilter(encryptedFilter);
                }
                break;
            default:
                break;
        }
        return encScan;
    }


    public Object parseFilter(RowFilter filter) {
        CompareFilter.CompareOp comp;
        ByteArrayComparable bComp;

        if(filter != null) {
            switch (this.cType) {
                case STD:
                case DET:
                    comp = filter.getOperator();
                    bComp = filter.getComparator();

                    Object[] parserResult = new Object[2];
                    parserResult[0] = comp;
                    parserResult[1] = bComp.getValue();

                    return parserResult;
                case OPE:
                    comp = filter.getOperator();
                    bComp = filter.getComparator();
                    BinaryComparator encBC = new BinaryComparator(this.encode(bComp.getValue()));

                    return new RowFilter(comp, encBC);
                default:
                    return null;
            }
        }
        else return null;
    }

}
