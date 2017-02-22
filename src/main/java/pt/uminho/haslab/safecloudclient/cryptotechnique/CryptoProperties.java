package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;

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

//        if(this.cType.equals(CryptoTechnique.CryptoType.OPE)) {
            if (startRow.length != 0 && stopRow.length != 0) {
                return new Scan(this.encode(startRow), this.encode(stopRow));
            } else if (startRow.length != 0 && stopRow.length == 0) {
                return new Scan(this.encode(startRow));
            } else if (startRow.length == 0 && stopRow.length == 0) {
                return new Scan();
            }
            else
//                TODO mudar isto
                return null;
//        }
//        else
//            return s;

    }


}
