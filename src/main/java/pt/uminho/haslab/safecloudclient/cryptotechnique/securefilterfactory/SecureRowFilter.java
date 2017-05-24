package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureRowFilter implements SecureFilterProperties {
    public CryptoProperties cryptoProperties;

    public SecureRowFilter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        RowFilter plainRowFilter = (RowFilter) plaintextFilter;
//      In the RowFilter case, the CryptoType is protecting the row key
        switch (cryptoType) {
            case PLT:
                return plainRowFilter;
            case STD:
            case DET:
            case FPE:
                return null;
            case OPE:
                byte[] encryptedRowKey = this.cryptoProperties.encodeRow(plainRowFilter.getComparator().getValue());
                BinaryComparator bc = new BinaryComparator(encryptedRowKey);
                return new RowFilter(plainRowFilter.getOperator(), bc);
        }
        return null;
    }

    @Override
    public Object parseFilter(Filter plaintextFilter) {
        return null;
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter) {
        return this.cryptoProperties.tableSchema.getKey().getCryptoType();
    }


}
