package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 5/23/17.
 */
public interface SecureFilterProperties {

    public Filter buildEncryptedFilter(Filter plaintextFilter);

    public Object parseFilter(Filter plaintextFilter);

    public CryptoTechnique.CryptoType getFilterCryptoType();

}
