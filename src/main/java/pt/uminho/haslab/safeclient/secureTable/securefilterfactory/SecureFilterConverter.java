package pt.uminho.haslab.safeclient.secureTable.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;

public class SecureFilterConverter implements SecureFilterProperties {

    CryptoProperties cryptoProperties;

    public SecureFilterConverter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    static FilterType getFilterType(Filter filter) {
        if (filter != null) {
            String filterClass = filter.getClass().getSimpleName();
            switch (filterClass) {
                case "RowFilter":
                    return FilterType.RowFilter;
                case "SingleColumnValueFilter":
                    return FilterType.SingleColumnValueFilter;
                case "FilterList":
                    return FilterType.FilterList;
                case "WhileMatchFilter":
                    return FilterType.WhileMatchFilter;
                default:
                    throw new UnsupportedOperationException("Filter class not supported.");
            }
        } else {
            throw new NullPointerException("Cannot get FilterType of a null Filter object");
        }
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoType cType) {
        FilterType fType = getFilterType(plaintextFilter);
        if (fType != null) {
            switch (fType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                case WhileMatchFilter:
                    return new SecureWhileMatchFilter(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                default:
                    throw new UnsupportedOperationException("Secure object not supported for the specified filter.");
            }
        } else {
            throw new UnsupportedOperationException("Secure operation not supported for the specified filter.");
        }
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoType cryptoType) {
        FilterType fType = getFilterType(plaintextFilter);
        if (fType != null) {
            switch (fType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                case WhileMatchFilter:
                    return new SecureWhileMatchFilter(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                default:
                    throw new UnsupportedOperationException("Secure object not supported for the specified filter.");
            }
        } else {
            throw new NullPointerException("FilterType cannot be null.");
        }
    }

    @Override
    public CryptoType getFilterCryptoType(Filter plaintextFilter) {
        FilterType filterType = getFilterType(plaintextFilter);
        if (filterType != null) {
            switch (filterType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                case WhileMatchFilter:
                    return new SecureWhileMatchFilter(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                default:
                    throw new UnsupportedOperationException("Secure object not supported for the specified filter.");
            }
        } else {
            throw new NullPointerException("FilterType cannot be null.");
        }
    }

    public enum FilterType {
        RowFilter,
        SingleColumnValueFilter,
        FilterList,
        WhileMatchFilter
    }


}
