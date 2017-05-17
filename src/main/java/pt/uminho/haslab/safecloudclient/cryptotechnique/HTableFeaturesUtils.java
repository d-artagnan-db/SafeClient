package pt.uminho.haslab.safecloudclient.cryptotechnique;

import javafx.util.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

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

    public List<String> deleteCells(CellScanner cs) {
        List<String> cellsToDelete = new ArrayList<>();
        try {
            while(cs.advance()) {
                Cell cell = cs.current();
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);

                if(family.length != 0 && qualifier.length != 0) {
                    cellsToDelete.add(new String(family)+"#"+new String(qualifier));
                }
                else if(family.length != 0) {
                    cellsToDelete.add(new String(family));
                }
            }
        } catch (IOException e) {
//            TODO falta por o LOG
            System.out.println("Exception in deleteCells CellScanner: "+e.getMessage());
        }
        return cellsToDelete;
    }

    public void wrapDeletedCells(List<String> cellsToDelete, Delete delete) {
        if(cellsToDelete.size() != 0) {
            for(String c : cellsToDelete) {
                String[] familiesAndQualifiers = c.split("#");
                if(familiesAndQualifiers.length == 1) {
                    delete.deleteFamily(familiesAndQualifiers[0].getBytes());
                } else if(familiesAndQualifiers.length == 2) {
                    delete.deleteColumns(familiesAndQualifiers[0].getBytes(), familiesAndQualifiers[1].getBytes());
                } else {
                    throw new IllegalArgumentException("Family or qualifier cannot contains # character.");
                }
            }
        }
    }
}
