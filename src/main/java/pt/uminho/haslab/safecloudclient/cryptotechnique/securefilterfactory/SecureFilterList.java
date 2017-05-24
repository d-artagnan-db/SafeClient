package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureFilterList implements SecureFilterProperties {
    CryptoProperties cryptoProperties;

    public SecureFilterList(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        return null;
    }

    @Override
    public Object parseFilter(Filter plaintextFilter) {
        return null;
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter) {
        return null;
    }
}
