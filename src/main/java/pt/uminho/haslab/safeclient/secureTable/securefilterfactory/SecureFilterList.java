package pt.uminho.haslab.safeclient.secureTable.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;

import java.util.ArrayList;
import java.util.List;


public class SecureFilterList implements SecureFilterProperties {
    CryptoProperties cryptoProperties;

    public SecureFilterList(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoType cryptoType) {
        FilterList plaintextFilterList = (FilterList) plaintextFilter;
        List<Filter> fList = plaintextFilterList.getFilters();
        List<Filter> encryptedFList = new ArrayList<>(fList.size());

        for(Filter f : fList) {
            Filter eFilter;
            switch(SecureFilterConverter.getFilterType(f)) {
                case RowFilter:
                    CryptoType rfCryptoType = new SecureRowFilter(this.cryptoProperties).getFilterCryptoType(f);
                    eFilter = new SecureRowFilter(this.cryptoProperties).buildEncryptedFilter(f, rfCryptoType);
                    if(eFilter == null) {
                        throw new UnsupportedOperationException("Filter operation not supported for the Cryptographic Techniques specified.");
                    } else {
                        encryptedFList.add(eFilter);
                    }
                    break;
                case SingleColumnValueFilter:
                    CryptoType scvCryptoType = new SecureSingleColumnValueFilter(this.cryptoProperties).getFilterCryptoType(f);
                    eFilter = new SecureSingleColumnValueFilter(this.cryptoProperties).buildEncryptedFilter(f, scvCryptoType);
                    if(eFilter == null) {
                        throw new UnsupportedOperationException("Filter operation not supported for the Cryptographic Techniques specified.");
                    } else {
                        encryptedFList.add(eFilter);
                    }
                    break;
                case FilterList:
                    encryptedFList.add(new SecureFilterList(this.cryptoProperties).buildEncryptedFilter(f, this.cryptoProperties.tableSchema.getKey().getCryptoType()));
                    break;
                default:
                    break;
            }
        }
        return new FilterList(plaintextFilterList.getOperator(), encryptedFList);
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoType cryptoType) {
        return null;
    }

    @Override
    public CryptoType getFilterCryptoType(Filter plaintextFilter) {
//        throw new NullPointerException("getFilterCryptoType: PORQUE É QUE EU FUI CHAMADO?????");
        return this.cryptoProperties.tableSchema.getKey().getCryptoType();
    }
}
