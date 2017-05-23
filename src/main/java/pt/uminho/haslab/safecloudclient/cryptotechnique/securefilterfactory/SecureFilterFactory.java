package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.util.List;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureFilterFactory {



    public Filter getSecureFilter(CryptoProperties cryptoProperties, Filter f) {
        if(f instanceof RowFilter) {
            return new SecureRowFilter(
                    cryptoProperties,
                    ((RowFilter) f).getOperator(),
                    ((RowFilter) f).getComparator());
        }
        else if(f instanceof SingleColumnValueFilter) {
            return new SecureSingleColumnValueFilter(
                    cryptoProperties,
                    ((SingleColumnValueFilter) f).getFamily(),
                    ((SingleColumnValueFilter) f).getQualifier(),
                    ((SingleColumnValueFilter) f).getOperator(),
                    ((SingleColumnValueFilter) f).getComparator().getValue());
        }
        else if(f instanceof FilterList) {
            List<Filter> encryptFilterList = new SecureFilterList(
                    cryptoProperties,
                    (FilterList) f).encryptFilter();
            return new FilterList(encryptFilterList);
        }
        else return null;
    }

}
