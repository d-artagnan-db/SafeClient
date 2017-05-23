package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureFilterList implements SecureFilterProperties {
    CryptoProperties cryptoProperties;
    FilterList plaintextFilterList;
    public SecureFilterList(CryptoProperties cryptoProperties, FilterList filterList) {
        this.cryptoProperties = cryptoProperties;
        this.plaintextFilterList = filterList;
    }

    public List<Filter> encryptFilter() {
        return new ArrayList<>();
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
        return null;
    }
}
