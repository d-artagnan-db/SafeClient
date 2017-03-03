
package pt.uminho.haslab.safecloudclient.tests;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.testingutils.ValuesGenerator;

public class ConcurrentPutGetSameKeysTest  extends ConcurrentPutGetTest{
    
    /**
     * This test changes the size of values and number of threads used in 
     * ConccurentPutGetTest
     * @return 
     */
    @Parameterized.Parameters
    public static Collection valueGenerator() {
        nThreads = 10;
        nValues = 100;
        LOG.debug("Going to generate values with "+ nThreads+ " nClients " + nValues + " nVals");
        return ValuesGenerator.concurrentPutGetGenerator(nValues, nThreads);
    }

    
    public ConcurrentPutGetSameKeysTest(List<BigInteger> testingValues, List<List<BigInteger>> testingIdentifiers) throws Exception {
        super(testingValues, testingIdentifiers);

        List<List<BigInteger>> newIdent = new ArrayList<List<BigInteger>>();

        for(int i=0; i < testingIdentifiers.size(); i++){
            List<BigInteger> idents = new ArrayList<BigInteger>();

            for(BigInteger ident: testingIdentifiers.get(0)){
                idents.add(ident);
            }
            newIdent.add(idents);
        }
        this.identifiers.clear();
        for(List<BigInteger> list: newIdent){
            this.identifiers.add(list);
        }
        
    }

}
