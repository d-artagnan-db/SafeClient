package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureRowFilter extends RowFilter implements SecureFilterProperties {
    public CryptoProperties cryptoProperties;

    public SecureRowFilter(CryptoProperties cryptoProperties, CompareOp rowCompareOp, ByteArrayComparable rowComparator) {
        super(rowCompareOp, rowComparator);
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter) {
        return null;
    }

    @Override
    public Object parseFilter(Filter plaintextFilter) {
        return null;
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType() {
        return this.cryptoProperties.tableSchema.getKey().getCryptoType();
    }


}
