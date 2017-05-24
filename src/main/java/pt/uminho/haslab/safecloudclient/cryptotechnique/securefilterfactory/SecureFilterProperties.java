package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 5/23/17.
 */
public interface SecureFilterProperties {

    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType);

    public Object parseFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType);

    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter);

}
