package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory.SecureFilterConverter;
import pt.uminho.haslab.safecloudclient.queryengine.QEngineIntegration;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by rgmacedo on 5/17/17.
 */
public class HTableFeaturesUtils {
    public CryptoProperties cp;
    public SecureFilterConverter secureFilterConverter;

    public HTableFeaturesUtils(CryptoProperties cryptoProperties, SecureFilterConverter secureFilterConverter) {
        cp = cryptoProperties;
        this.secureFilterConverter = secureFilterConverter;
    }

    public void encryptCells(CellScanner cs, TableSchema tableSchema, Put destination, CryptoProperties cryptoProperties) {
        try {
            while (cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);

                boolean verifyProperty = false;
                String qualifierString = new String(qualifier, Charset.forName("UTF-8"));
                String opeValues = "_STD";

    //				Verify if the actual qualifier corresponds to the supporting qualifier (<qualifier>_STD)
                if (qualifierString.length() >= opeValues.length()) {
                    verifyProperty = qualifierString.substring(qualifierString.length() - opeValues.length(), qualifierString.length()).equals(opeValues);
                }
                if (!verifyProperty) {
    //					Encode the original value with the corresponding CryptoBox
                    destination.add(
                            family,
                            qualifier,
                            cryptoProperties.encodeValue(
                                    family,
                                    qualifier,
                                    value));

    //					If the actual qualifier CryptoType is equal to OPE, encode the same value with STD CryptoBox
                    if (tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), qualifierString) == CryptoTechnique.CryptoType.OPE) {
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
            }
        } catch (IOException e) {
//            TODO falta mandar para o LOG
            System.out.println("Exception in cell scanner. " + e.getMessage());
        }
    }

    public List<String> deleteCells(CellScanner cs) {
        List<String> cellsToDelete = new ArrayList<>();
        try {
            while(cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                if(family.length != 0 && qualifier.length != 0) {
                    cellsToDelete.add(new String(family)+"#"+new String(qualifier));
                }
                else if(family.length != 0) {
                    cellsToDelete.add(new String(family));
                }
            }
        } catch (IOException e) {
//            TODO falta por o LOG
            System.out.println("Exception in deleteCells CellScanner: "+e.getMessage());
        }
        return cellsToDelete;
    }

    public void wrapDeletedCells(List<String> cellsToDelete, Delete delete) {
        if(cellsToDelete.size() != 0) {
            for(String c : cellsToDelete) {
                String[] familiesAndQualifiers = c.split("#");
                if(familiesAndQualifiers.length == 1) {
                    System.out.println("Só tem family");
                    delete.deleteFamily(familiesAndQualifiers[0].getBytes());
                } else if(familiesAndQualifiers.length == 2) {
                    System.out.println("Tem family e qualifier");
                    delete.deleteColumns(familiesAndQualifiers[0].getBytes(), familiesAndQualifiers[1].getBytes());
                } else {
                    throw new IllegalArgumentException("Family or qualifier cannot contains # character.");
                }
            }
        }
    }

    /**
     * isScanOrFilter(scan : Scan) method : check the CryptoType of a scan operation. It may vary if it's a Scan, Row Filter or SingleColumnValueFilter
     * @param scan scan object used to check the instance
     * @return the Scan's CryptoType (Row Key CryptoType in case of Scan or RowFilter, Qualifier CryptoType in case of SingleColumnValueFilter)
     */
    public CryptoTechnique.CryptoType isScanOrFilter(Scan scan) {
        if(scan.hasFilter()) {
            Filter filter = scan.getFilter();
            if(filter instanceof RowFilter) {
                return cp.tableSchema.getKey().getCryptoType();
            }
            else if(scan.getFilter() instanceof SingleColumnValueFilter) {
                SingleColumnValueFilter singleColumn = (SingleColumnValueFilter) filter;
                String family = new String(singleColumn.getFamily(), Charset.forName("UTF-8"));
                String qualifier = new String(singleColumn.getQualifier(), Charset.forName("UTF-8"));
                return cp.tableSchema.getCryptoTypeFromQualifier(family, qualifier);
            }
            else {
                return null;
            }
//            return this.secureFilterFactory.getSecureFilter(this.cp, scan.getFilter()).getFilterCryptoType();
        }
        else {
            return cp.tableSchema.getKey().getCryptoType();
        }
    }

    /**
     * encodeDelimitingRows(encScan : Scan, startRow : byte[], stopRow : byte[]) method : set the encrypted start and stop rows to an encrypted scan operator
     * @param encScan encrytped scan operator
     * @param startRow original start row
     * @param stopRow original stop row
     * @return an encrypted scan with the respective start and stop row, both encrypted with the row key CryptoBox
     */
    public Scan encodeDelimitingRows(Scan encScan, byte[] startRow, byte[] stopRow) {
        if (startRow.length != 0 && stopRow.length != 0) {
            encScan.setStartRow(cp.encodeRow(startRow));
            encScan.setStopRow(cp.encodeRow(stopRow));
        } else if (startRow.length != 0 && stopRow.length == 0) {
            encScan.setStartRow(cp.encodeRow(startRow));
        } else if (startRow.length == 0 && stopRow.length != 0) {
            encScan.setStopRow(cp.encodeRow(stopRow));
        }
        return encScan;
    }

    /**
     * encryptedScan(s : Scan) : convert a regular Scan object in the respective encrypted object
     * @param s scan object
     * @return the respective encrypted scan object
     */
    public Scan encryptedScan(Scan s) {
        byte[] startRow = s.getStartRow();
        byte[] stopRow = s.getStopRow();
        Scan encScan = null;

//		get the CryptoType of the Scan/Filter operation
        CryptoTechnique.CryptoType scanCryptoType = isScanOrFilter(s);
//		Map the database column families and qualifiers into a collection
        Map<byte[], List<byte[]>> columns = cp.getFamiliesAndQualifiers(s.getFamilyMap());

        switch (scanCryptoType) {
//			In case of plaintext, return the same object as received
            case PLT :
                encScan = s;
                break;
//			In case of standard or deterministic encryption, since no order is preserved a full table scan must be performed.
//			In case of Filter, the compare value must be encrypted.
            case STD :
            case DET :
            case FPE :
                encScan = new Scan();
//				Add only the specified qualifiers in the original scan (s), instead of retrieve all (unnecessary) values).
                for(byte[] f : columns.keySet()) {
                    List<byte[]> qualifiersTemp = columns.get(f);
                    for(byte[] q : qualifiersTemp) {
                        encScan.addColumn(f, q);
                    }
                }
//				Since the scanCryptoType defines the CryptoType of the scan or filter operaion, in case of SingleColumnValueFilter,
// 				the start and stop row must be encoded with the respective row key CryptoBox
                if((cp.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.PLT) ||
                        (cp.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.OPE)) {
                    encScan = encodeDelimitingRows(encScan, startRow, stopRow);
                }

//				In case of filter, the compare value must be encrypted
                if(s.hasFilter()) {
                    if(s.getFilter() instanceof SingleColumnValueFilter) {
//						System.out.println("Entrou no singlecolumn cenas");
                        SingleColumnValueFilter f = (SingleColumnValueFilter) s.getFilter();
                        ByteArrayComparable bComp = f.getComparator();
                        byte[] value = bComp.getValue();

                        encScan.setFilter(new SingleColumnValueFilter(f.getFamily(), f.getQualifier(), f.getOperator(), cp.encodeRow(value)));
//						System.out.println("Fez set Filter");
                    }
                }

                break;
            case OPE :
                encScan = new Scan();
//				Add only the specified qualifiers in the original scan (s), instead of retrieve all (unnecessary) values).
                for(byte[] f : columns.keySet()) {
                    List<byte[]> qualifiersTemp = columns.get(f);
                    for(byte[] q : qualifiersTemp) {
                        encScan.addColumn(f, q);
                    }
                }
//				Since the scanCryptoType defines the CryptoType of the scan or filter operaion, in case of SingleColumnValueFilter,
// 				the start and stop row must be encoded with the respective row key CryptoBox
                if((cp.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.PLT) ||
                        (cp.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.OPE)) {
                    encScan = encodeDelimitingRows(encScan, startRow, stopRow);
                }
                if (s.hasFilter()) {
                    Filter encryptedFilter = (Filter) parseFilter(s.getFilter());
                    encScan.setFilter(encryptedFilter);
                }
                break;
            default :
                break;
        }
        return encScan;
    }

    /**
     * parseFilter(filter : Filter) method : when setting a filter, parse it and handle it according the respective CryptoType
     * @param filter Filter object
     * @return provide an encrypted Filter, with the respective operator and compare value.
     */
    public Object parseFilter(Filter filter) {
        CompareFilter.CompareOp comp;
        ByteArrayComparable bComp;
        Object returnValue = null;

        if(filter != null) {
            if(filter instanceof RowFilter) {
                RowFilter rowFilter = (RowFilter) filter;
                comp = rowFilter.getOperator();
                bComp = rowFilter.getComparator();

                switch (cp.tableSchema.getKey().getCryptoType()) {
                    case PLT :
                        returnValue = rowFilter;
                        break;
                    case STD :
                    case DET :
                    case FPE :
                        Object[] parserResult = new Object[2];
                        parserResult[0] = comp;
                        parserResult[1] = bComp.getValue();

                        returnValue = parserResult;
                        break;
                    case OPE :
//						Generate a Binary Comparator to perform the comparison with the respective encrypted value
                        BinaryComparator encBC = new BinaryComparator(cp.encodeRow(bComp.getValue()));
                        returnValue = new RowFilter(comp, encBC);
                        break;
                    default:
                        returnValue = null;
                        break;
                }
            }
            else if(filter instanceof SingleColumnValueFilter) {
                SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) filter;
                byte[] family = singleFilter.getFamily();
                byte[] qualifier = singleFilter.getQualifier();
                comp = singleFilter.getOperator();
                bComp = singleFilter.getComparator();

                switch (cp.tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), new String(qualifier, Charset.forName("UTF-8")))) {
                    case PLT :
                        returnValue = singleFilter;
                        break;
                    case STD :
                    case DET :
                    case FPE :
                        Object[] parserResult = new Object[4];
                        parserResult[0] = family;
                        parserResult[1] = qualifier;
                        parserResult[2] = comp;
                        parserResult[3] = bComp.getValue();

                        returnValue = parserResult;
                        break;
                    case OPE :
//						Generate a Binary Comparator to perform the comparison with the respective encrypted value
                        BinaryComparator encBC = new BinaryComparator(cp.encodeValue(family, qualifier, bComp.getValue()));
                        returnValue = new SingleColumnValueFilter(family, qualifier, comp, encBC);
                        break;
                    default:
                        returnValue = null;
                        break;
                }
            }
            else {
                throw new UnsupportedOperationException("Secure filter operation not supported.");
            }
        }

        return returnValue;
    }

    /**
     * verifyFilterCryptoType(scan : Scan) method : verify the filter's CryptoBox
     * @param scan scan/filter object
     * @return the respective CryptoType
     */
    public CryptoTechnique.CryptoType verifyFilterCryptoType(Scan scan) {
        CryptoTechnique.CryptoType cryptoType = cp.tableSchema.getKey().getCryptoType();

        if(scan.hasFilter()) {
            Filter filter = scan.getFilter();
            if(filter instanceof SingleColumnValueFilter) {
                String family = new String(((SingleColumnValueFilter) filter).getFamily(), Charset.forName("UTF-8"));
                String qualifier = new String(((SingleColumnValueFilter)filter).getQualifier(), Charset.forName("UTF-8"));
                cryptoType = cp.tableSchema.getCryptoTypeFromQualifier(family, qualifier);
            }
        }
        return cryptoType;
    }


    public void createDynamicColumnsForAtomicOperations(QEngineIntegration qEngine, TableSchema tableSchema, String family, String qualifier){

//		In case of default schema, verify and/or create both family and qualifier instances in TableSchema
        if (!qEngine.doesColumnFamilyExist(tableSchema, family)) {
            tableSchema.addFamily(qEngine.createDefaultFamily(family));
        }

        if(qualifier != null) {
            if (!qEngine.doesFamilyContainsQualifier(tableSchema, family, qualifier)) {
                tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier, CryptoTechnique.CryptoType.OPE));
                tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier + "_STD", CryptoTechnique.CryptoType.STD));
                cp.replaceQualifierCryptoHandler(family, qualifier, qEngine.getCryptographicTechnique(), qEngine.getFamilyFormatSize());
            }
        }

    }

}
