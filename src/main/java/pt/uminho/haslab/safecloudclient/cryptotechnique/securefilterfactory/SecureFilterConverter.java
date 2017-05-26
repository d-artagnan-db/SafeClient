package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.util.List;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureFilterConverter implements SecureFilterProperties{

    CryptoProperties cryptoProperties;

    public enum FilterType {
        RowFilter,
        SingleColumnValueFilter,
        FilterList
    }

    public SecureFilterConverter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }




    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cType) {
        System.out.println("Filter Type: "+this.getFilterType(plaintextFilter));
        FilterType fType = getFilterType(plaintextFilter);
        if(fType != null) {
            switch (fType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).buildEncryptedFilter(plaintextFilter, cType);
                default:
                    return null;
            }
        }
        else {
            throw new UnsupportedOperationException("Secure operation not supported for the specified filter.");
        }
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        FilterType fType = getFilterType(plaintextFilter);
        if(fType != null) {
            switch (fType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).parseFilter(plaintextFilter, cryptoType);
                default:
                    return null;
            }
        }
        else {
            throw new UnsupportedOperationException("Secure operation not supported for the specified filter.");
        }
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter) {
        FilterType filterType = getFilterType(plaintextFilter);
        if(filterType != null) {
            switch (filterType) {
                case RowFilter:
                    return new SecureRowFilter(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                case SingleColumnValueFilter:
                    return new SecureSingleColumnValueFilter(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                case FilterList:
                    return new SecureFilterList(this.cryptoProperties).getFilterCryptoType(plaintextFilter);
                default:
                   return null;
            }
        }
        else {
            throw new UnsupportedOperationException("Secure operation not supported for the specified filter.");
        }
    }

    static FilterType getFilterType(Filter filter) {
        if(filter != null) {
            switch (filter.getClass().getSimpleName()) {
                case "RowFilter":
                    return FilterType.RowFilter;
                case "SingleColumnValueFilter":
                    return FilterType.SingleColumnValueFilter;
                case "FilterList":
                    return FilterType.FilterList;
                default:
                    return null;
            }
        }
        else {
            throw new NullPointerException("Cannot get FilterType of a null Filter object");
        }
    }



}
