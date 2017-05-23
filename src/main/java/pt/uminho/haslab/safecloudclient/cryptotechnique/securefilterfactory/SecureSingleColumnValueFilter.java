package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureSingleColumnValueFilter extends SingleColumnValueFilter implements SecureFilterProperties {
    public CryptoProperties cryptoProperties;
    public byte[] family;
    public byte[] qualifier;

    public SecureSingleColumnValueFilter(CryptoProperties cryptoProperties, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value) {
        super(family, qualifier, compareOp, value);
        this.cryptoProperties = cryptoProperties;
        this.family = family;
        this.qualifier = qualifier;
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
        return this.cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier));
    }
}
