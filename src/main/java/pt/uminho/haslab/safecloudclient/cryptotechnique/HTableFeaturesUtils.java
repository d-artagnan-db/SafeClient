package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created by rgmacedo on 5/17/17.
 */
public class HTableFeaturesUtils {

    public HTableFeaturesUtils() {

    }

    public void encryptCells(CellScanner cs, TableSchema tableSchema, Put encryptedPut, CryptoProperties cryptoProperties) {
        try {
            while (cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);

                boolean verifyProperty = false;
                String qualifierString = new String(qualifier, Charset.forName("UTF-8"));
                String opeValues = "_STD";

    //				Verify if the actual qualifier corresponds to the supporting qualifier (<qualifier>_STD)
                if (qualifierString.length() >= opeValues.length()) {
                    verifyProperty = qualifierString.substring(qualifierString.length() - opeValues.length(), qualifierString.length()).equals(opeValues);
                }
                if (!verifyProperty) {
    //					Encode the original value with the corresponding CryptoBox
                    encryptedPut.add(
                            family,
                            qualifier,
                            cryptoProperties.encodeValue(
                                    family,
                                    qualifier,
                                    value));

    //					If the actual qualifier CryptoType is equal to OPE, encode the same value with STD CryptoBox
                    if (tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), qualifierString) == CryptoTechnique.CryptoType.OPE) {
                        encryptedPut.add(
                                family,
                                (qualifierString + opeValues).getBytes(Charset.forName("UTF-8")),
                                cryptoProperties.encodeValue(
                                        family,
                                        (qualifierString + opeValues).getBytes(Charset.forName("UTF-8")),
                                        value)
                        );
                    }

                }
            }
        } catch (IOException e) {
//            TODO falta mandar para o LOG
            System.out.println("Exception in cell scanner. " + e.getMessage());
        }
    }

}
