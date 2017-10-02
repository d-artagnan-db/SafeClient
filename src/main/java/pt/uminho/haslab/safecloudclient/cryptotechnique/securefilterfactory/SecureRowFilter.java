package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureRowFilter implements SecureFilterProperties {
    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
    public CryptoProperties cryptoProperties;

    public SecureRowFilter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        RowFilter plainRowFilter = (RowFilter) plaintextFilter;
//      In the RowFilter case, the CryptoType is protecting the Row-Key
        switch (cryptoType) {
            case STD:
            case DET:
            case FPE:
                LOG.error("SecureRowFilter.class:buildEncryptedFilter:UnsupportedOperationException: RowFilter operation not supported for non-order-preserving CryptoBoxes.");
                throw new UnsupportedOperationException("RowFilter operation not supported for non-order-preserving CryptoBoxes.");
            case PLT:
            case OPE:
                byte[] encryptedRowKey = this.cryptoProperties.encodeRow(plainRowFilter.getComparator().getValue());
                ByteArrayComparable bc = this.cryptoProperties.checkComparatorType(plainRowFilter.getComparator(), encryptedRowKey, cryptoType);
                return new RowFilter(plainRowFilter.getOperator(), bc);
            default:
                return null;
        }
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        RowFilter plainRowFilter = (RowFilter) plaintextFilter;

        switch (cryptoType) {
            case STD :
            case DET :
            case FPE :
//                TODO: adicionar parserResult[0] = RowFilter?
                Object[] parserResult = new Object[2];
                parserResult[0] = plainRowFilter.getOperator();
                parserResult[1] = plainRowFilter.getComparator().getValue();

                return parserResult;
            case PLT :
            case OPE :
//				Generate a Binary Comparator to perform the comparison with the respective encrypted value
                return buildEncryptedFilter(plaintextFilter, cryptoType);

            default:
                return null;
        }
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter) {
        return this.cryptoProperties.tableSchema.getKey().getCryptoType();
    }


}
