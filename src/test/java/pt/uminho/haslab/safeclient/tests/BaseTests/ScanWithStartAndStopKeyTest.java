package pt.uminho.haslab.safeclient.tests.BaseTests;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;

import java.util.Arrays;
import java.util.Random;

public class ScanWithStartAndStopKeyTest extends MultiColumnProtectedTest {


    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int firstIndex  = r.nextInt(rowIdentifiers.size());
        int secondIndex = r.nextInt(rowIdentifiers.size());
        byte[] firstValue = rowIdentifiers.get(firstIndex);
        byte[] secondValue = rowIdentifiers.get(secondIndex);
        BinaryComparator bc = new BinaryComparator(firstValue);
        LOG.debug("Chosen indexes are " + Arrays.toString(firstValue) + ": " + Arrays.toString(secondValue) + " = " + bc.compareTo(secondValue));

        if(bc.compareTo(secondValue) > 0){
            byte[] aux = firstValue;
            firstValue = secondValue ;
            secondValue = aux;
        }
        LOG.debug("Chosen indexes are " + Arrays.toString(firstValue) + ": " + Arrays.toString(secondValue));

        return new Scan(firstValue, secondValue);
    }

}
