package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;


public interface SecureFilterProperties {

    Filter buildEncryptedFilter(Filter plaintextFilter, CryptoType cryptoType);

    Object parseFilter(Filter plaintextFilter, CryptoType cryptoType);

    CryptoType getFilterCryptoType(Filter plaintextFilter);

}
