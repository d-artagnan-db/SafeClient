package pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

/**
 * Created by rgmacedo on 5/23/17.
 */
public class SecureSingleColumnValueFilter implements SecureFilterProperties {
    public CryptoProperties cryptoProperties;

    public SecureSingleColumnValueFilter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoTechnique.CryptoType cryptoType) {
        SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) plaintextFilter;

        switch (cryptoType) {
            case PLT:
                return singleFilter;
            case STD:
                return null;
            case DET:
            case FPE:
            case OPE:
                byte[] encryptedValue =
                        this.cryptoProperties.encodeValue(
                            singleFilter.getFamily(),
                            singleFilter.getQualifier(),
                            singleFilter.getComparator().getValue());

                return new SingleColumnValueFilter(
                        singleFilter.getFamily(),
                        singleFilter.getQualifier(),
                        singleFilter.getOperator(),
                        new BinaryComparator(encryptedValue));
            default:
                return null;
        }
    }

    @Override
    public Object parseFilter(Filter plaintextFilter) {
        return null;
    }

    @Override
    public CryptoTechnique.CryptoType getFilterCryptoType(Filter plaintextFilter) {
        SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) plaintextFilter;
        return this.cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(singleFilter.getFamily()), new String(singleFilter.getQualifier()));
    }
}
