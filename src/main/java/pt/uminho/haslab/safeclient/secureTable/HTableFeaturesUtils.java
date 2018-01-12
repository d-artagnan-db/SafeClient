package pt.uminho.haslab.safeclient.secureTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import pt.uminho.haslab.safeclient.secureTable.securefilterfactory.SecureFilterConverter;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by rgmacedo on 5/17/17.
 */
public class HTableFeaturesUtils {
    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());

    private CryptoProperties cp;
    private SecureFilterConverter secureFilterConverter;

    public HTableFeaturesUtils(CryptoProperties cryptoProperties, SecureFilterConverter secureFilterConverter) {
        this.cp = cryptoProperties;
        this.secureFilterConverter = secureFilterConverter;
    }


    public void encryptCells(CellScanner cs, TableSchema tableSchema, Put destination, CryptoProperties cryptoProperties) {
        try {
            while (cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);

                String qualifierString = new String(qualifier, Charset.forName("UTF-8"));
                String opeValues = "_STD";

//				Encode the original value with the corresponding CryptoBox
                destination.add(family, qualifier, cryptoProperties.encodeValue(family, qualifier, value));

//				If the actual qualifier CryptoType is equal to OPE, encode the same value with STD CryptoBox
                if (tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), qualifierString) == CryptoType.OPE) {
                    destination.add(
                            family,
                            (qualifierString + opeValues).getBytes(Charset.forName("UTF-8")),
                            cryptoProperties.encodeValue(
                                    family,
                                    (qualifierString + opeValues).getBytes(Charset.forName("UTF-8")),
                                    value)
                        );
                    }

            }
        } catch (Exception e) {
            LOG.error("Exception in cell scanner. " + e.getMessage());
        }
    }

    public List<String> deleteCells(CellScanner cs) {
        List<String> cellsToDelete = new ArrayList<>();
        try {
            while (cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                if (family.length != 0 && qualifier.length != 0) {
                    if (this.cp.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier)) == CryptoType.OPE) {
                        cellsToDelete.add(new String(family) + "#" + new String(qualifier));
                        cellsToDelete.add(new String(family) + "#" + new String(qualifier) + "_STD");
                    } else {
                        cellsToDelete.add(new String(family) + "#" + new String(qualifier));
                    }
                } else if (family.length != 0) {
                    cellsToDelete.add(new String(family));
                }
            }
        } catch (IOException e) {
            LOG.error("Exception in deleteCells CellScanner: " + e.getMessage());
        }
        return cellsToDelete;
    }

    public void wrapDeletedCells(List<String> cellsToDelete, Delete delete) {
        if (cellsToDelete.size() != 0) {
            for (String c : cellsToDelete) {
                String[] familiesAndQualifiers = c.split("#");
                if (familiesAndQualifiers.length == 1) {
                    delete.deleteFamily(familiesAndQualifiers[0].getBytes());
                } else if (familiesAndQualifiers.length == 2) {
                    delete.deleteColumns(familiesAndQualifiers[0].getBytes(), familiesAndQualifiers[1].getBytes());
                } else {
                    throw new IllegalArgumentException("Family or qualifier cannot contains # character.");
                }
            }
        }
    }

    /**
     * isScanOrFilter(scan : Scan) method : check the CryptoType of a scan operation. It may vary if it's a Scan, Row Filter or SingleColumnValueFilter
     *
     * @param scan scan object used to check the instance
     * @return the Scan's CryptoType (Row-Key CryptoType in case of Scan or RowFilter, Qualifier CryptoType in case of SingleColumnValueFilter)
     */
    private CryptoType getScanObjectCryptoType(Scan scan) {
        if (scan.hasFilter()) {
            return secureFilterConverter.getFilterCryptoType(scan.getFilter());
        } else {
            return cp.tableSchema.getKey().getCryptoType();
        }
    }

    /**
     * encodeDelimitingRows(encScan : Scan, startRow : byte[], stopRow : byte[]) method : set the encrypted start and stop rows to an encrypted scan operator
     *
     * @param encScan  encrytped scan operator
     * @param startRow original start row
     * @param stopRow  original stop row
     * @return an encrypted scan with the respective start and stop row, both encrypted with the Row-Key CryptoBox
     */
    private Scan encodeDelimitingRows(Scan encScan, byte[] startRow, byte[] stopRow) {
        if (startRow != null && startRow.length > 0 && stopRow != null && stopRow.length > 0) {
            encScan.setStartRow(cp.encodeRow(startRow));
            encScan.setStopRow(cp.encodeRow(stopRow));
        } else if (startRow != null && startRow.length > 0) {
            encScan.setStartRow(cp.encodeRow(startRow));
        } else if (stopRow != null && stopRow.length > 0) {
            encScan.setStopRow(cp.encodeRow(stopRow));
        }
        return encScan;
    }

    /**
     * encryptedScan(s : Scan) : convert a regular Scan object in the respective encrypted object
     *
     * @param s scan object
     * @return the respective encrypted scan object
     */
    public Scan buildEncryptedScan(Scan s) {
        byte[] startRow = s.getStartRow();
        byte[] stopRow = s.getStopRow();
        Scan encryptedScan = null;


        if (s.hasFilter() && (s.getFilter() instanceof FilterList)) {
            Filter encryptedFilter = secureFilterConverter.buildEncryptedFilter(s.getFilter(), this.cp.tableSchema.getKey().getCryptoType());
            encryptedScan = new Scan();
            Map<byte[], List<byte[]>> cols = this.cp.getHColumnDescriptors(s.getFamilyMap());

            this.wrapHColumnDescriptors(encryptedScan, cols);

            switch (this.cp.tableSchema.getKey().getCryptoType()) {
                case SMPC:
                case ISMPC:
                case PLT:
                case OPE:
                    encryptedScan = encodeDelimitingRows(encryptedScan, s.getStartRow(), s.getStopRow());
                    break;
                case STD:
                case DET:
                case FPE:
                default:
                    break;
            }

            encryptedScan.setFilter(encryptedFilter);

        } else {

//		    get the CryptoType of the Scan/Filter operation
            CryptoType scanCryptoType = this.getScanObjectCryptoType(s);
//		    Map the database column families and qualifiers into a collection
            Map<byte[], List<byte[]>> hColumnDescriptors = this.cp.getHColumnDescriptors(s.getFamilyMap());

            encryptedScan = new Scan();
//			Add only the specified qualifiers in the original scan (s), instead of retrieve all (unnecessary) values).
            this.wrapHColumnDescriptors(encryptedScan, hColumnDescriptors);

//	    	Since the scanCryptoType defines the CryptoType of the scan or filter operaion, in case of SingleColumnValueFilter,
//		    the start and stop row must be encoded with the respective Row-Key CryptoBox
            if ((this.cp.tableSchema.getKey().getCryptoType() == CryptoType.PLT) ||
                    (this.cp.tableSchema.getKey().getCryptoType() == CryptoType.OPE)) {
                encryptedScan = encodeDelimitingRows(encryptedScan, startRow, stopRow);
            }

            if (s.hasFilter()) {
                Filter encryptedFilter = this.secureFilterConverter.buildEncryptedFilter(s.getFilter(), scanCryptoType);
                if (encryptedFilter != null) {
                    encryptedScan.setFilter(encryptedFilter);
                }
            }

        }
        return encryptedScan;
    }

    /**
     * parseFilter(filter : Filter) method : when setting a filter, parse it and handle it according the respective CryptoType
     *
     * @param filter Filter object
     * @return provide an encrypted Filter, with the respective operator and compare value.
     */
    Object parseFilter(Filter filter) {
        Object returnValue = null;

        if (filter != null) {
            CryptoType cType = this.secureFilterConverter.getFilterCryptoType(filter);
            returnValue = this.secureFilterConverter.parseFilter(filter, cType);
        }

        return returnValue;
    }

    /**
     * verifyFilterCryptoType(scan : Scan) method : verify the filter's CryptoBox
     * @param scan scan/filter object
     * @return the respective CryptoType
     */
    CryptoType verifyFilterCryptoType(Scan scan) {

        CryptoType cryptoType = cp.tableSchema.getKey().getCryptoType();
        if (scan.hasFilter()) {
            cryptoType = this.secureFilterConverter.getFilterCryptoType(scan.getFilter());
        }
        return cryptoType;
    }

    void wrapHColumnDescriptors(Get object, Map<byte[], List<byte[]>> columns) {
        for (byte[] f : columns.keySet()) {
            if (columns.get(f) == null) {
                object.addFamily(f);
            } else {
                for (byte[] q : columns.get(f)) {
                    object.addColumn(f, q);
                }
                object.addFamily(f);
            }
        }
    }

    void wrapHColumnDescriptors(Scan object, Map<byte[], List<byte[]>> columns) {
        for (byte[] f : columns.keySet()) {
            if (columns.get(f) == null) {
                object.addFamily(f);
            } else {
                for (byte[] q : columns.get(f)) {
                    object.addColumn(f, q);
                }
                object.addFamily(f);
            }
        }
    }

    byte[] getObjectRow(Row object) {
        byte[] row = object.getRow();
        this.verifyNullableByteArray(row);

        return row;
    }

    void verifyNullableByteArray(byte[] content) {
        if (content.length == 0) {
            throw new NullPointerException("Byte[] cannot be null.");
        }
    }

}
