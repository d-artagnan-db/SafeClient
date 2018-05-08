package pt.uminho.haslab.safeclient.secureTable.securefilterfactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;
import pt.uminho.haslab.safeclient.secureTable.CryptoTable;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;


public class SecureWhileMatchFilter implements SecureFilterProperties {
    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
    public CryptoProperties cryptoProperties;

    public SecureWhileMatchFilter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }


    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoType cryptoType) {
        WhileMatchFilter whileFilter = (WhileMatchFilter) plaintextFilter;
        Filter insideFilter = whileFilter.getFilter();
        Filter tempFilter;
        switch (SecureFilterConverter.getFilterType(insideFilter)) {
            case RowFilter:
                tempFilter = new SecureRowFilter(this.cryptoProperties).buildEncryptedFilter(insideFilter, cryptoType);
                break;
            case SingleColumnValueFilter:
                tempFilter = new SecureSingleColumnValueFilter(this.cryptoProperties).buildEncryptedFilter(insideFilter, cryptoType);
                break;
            case FilterList:
                tempFilter = new SecureFilterList(this.cryptoProperties).buildEncryptedFilter(insideFilter, cryptoType);
                break;
            default:
                LOG.error("SecureMatchFilter.class:buildEncryptedFilter: Filter operator not supported.");
                throw new UnsupportedOperationException("Filter operator not supported.");
        }

        return new WhileMatchFilter(tempFilter);
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoType cryptoType) {
        WhileMatchFilter whileFilter = (WhileMatchFilter) plaintextFilter;
        Filter insideFilter = whileFilter.getFilter();
        switch (SecureFilterConverter.getFilterType(insideFilter)) {
            case RowFilter:
                return new SecureRowFilter(this.cryptoProperties).parseFilter(insideFilter, cryptoType);
            case SingleColumnValueFilter:
                return new SecureSingleColumnValueFilter(this.cryptoProperties).parseFilter(insideFilter, cryptoType);
            case FilterList:
                return new SecureFilterList(this.cryptoProperties).parseFilter(insideFilter, cryptoType);
            default:
                LOG.error("SecureWhileMatchFilter.class:parseFilter: Filter operator not supported.");
                throw new UnsupportedOperationException("Filter operator not suported.");
        }
    }

    @Override
    public CryptoType getFilterCryptoType(Filter plaintextFilter) {
        WhileMatchFilter whileFilter = (WhileMatchFilter) plaintextFilter;
        Filter f = whileFilter.getFilter();

        if (f != null) {
            switch (SecureFilterConverter.getFilterType(f)) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).getFilterCryptoType(f);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).getFilterCryptoType(f);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).getFilterCryptoType(f);
                default:
                    LOG.error("SecureMatchFilter.class:getFilterCryptoType: Filter operator not supported.");
                    throw new UnsupportedOperationException("Filter not supported.");
            }
        } else {
            LOG.error("SecureMatchFilter.class:getFilterCryptoType: WhilematchFilter cannot contain a null filter object.");
            throw new NullPointerException("WhileMatchFilter cannot contain a null filter object.");
        }
    }
}
