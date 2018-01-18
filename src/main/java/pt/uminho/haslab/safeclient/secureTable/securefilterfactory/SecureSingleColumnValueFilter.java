package pt.uminho.haslab.safeclient.secureTable.securefilterfactory;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;


public class SecureSingleColumnValueFilter implements SecureFilterProperties {
    public CryptoProperties cryptoProperties;

    public SecureSingleColumnValueFilter(CryptoProperties cryptoProperties) {
        this.cryptoProperties = cryptoProperties;
    }

    @Override
    public Filter buildEncryptedFilter(Filter plaintextFilter, CryptoType cryptoType) {
        SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) plaintextFilter;

        switch (cryptoType) {
            case STD:
                throw new UnsupportedOperationException("SingleColumnValueFilter is not supported for values protected with Standard Encryption (STD).");
            case DET:
            case FPE:
                if(singleFilter.getOperator() == CompareFilter.CompareOp.EQUAL) {
                    byte[] encryptedValue =
                            this.cryptoProperties.encodeValue(
                                    singleFilter.getFamily(),
                                    singleFilter.getQualifier(),
                                    singleFilter.getComparator().getValue());

                    return new SingleColumnValueFilter(
                            singleFilter.getFamily(),
                            singleFilter.getQualifier(),
                            singleFilter.getOperator(),
                            this.cryptoProperties.checkComparatorType(singleFilter.getComparator(), encryptedValue, cryptoType));
                }
                else {
                    throw new UnsupportedOperationException("Only equality comparison is supported for values protected with Deterministic-based encryption schemes.");
                }
            case SMPC:
            case ISMPC:
            case LSMPC:
            case PLT:
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
                        this.cryptoProperties.checkComparatorType(singleFilter.getComparator(), encryptedValue, cryptoType));
            default:
                return null;
        }
    }

    @Override
    public Object parseFilter(Filter plaintextFilter, CryptoType cryptoType) {
        SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) plaintextFilter;

        switch (cryptoType) {
            case STD :
                return null;
            case DET :
            case FPE :
                // TODO: adicionar parserResult[0] = SingleColumnValueFilter e remover o getFamily e getQualifier??
                Object[] parserResult = new Object[4];
                parserResult[0] = singleFilter.getFamily();
                parserResult[1] = singleFilter.getQualifier();
                parserResult[2] = singleFilter.getOperator();
                parserResult[3] = singleFilter.getComparator().getValue();

                return parserResult;
            case SMPC:
            case ISMPC:
            case LSMPC:
            case PLT:
            case OPE :
                return buildEncryptedFilter(plaintextFilter, cryptoType);

            default:
                return null;

        }
    }

    @Override
    public CryptoType getFilterCryptoType(Filter plaintextFilter) {
        SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) plaintextFilter;
        return this.cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(singleFilter.getFamily()), new String(singleFilter.getQualifier()));
    }
}
