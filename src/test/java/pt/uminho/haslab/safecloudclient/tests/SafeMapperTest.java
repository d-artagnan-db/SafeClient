package pt.uminho.haslab.safecloudclient.tests;

import org.junit.Test;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

public class SafeMapperTest {

    SchemaParser parser;

    public SafeMapperTest() {
        parser = new SchemaParser();
    }


    @Test
    public void testSchemaParsing() {
        String filename = "src/main/resources/q_engine.xml";
        parser.parseDatabaseTables(filename);
        System.out.println(parser.printDatabaseSchemas());
    }

    @Test
    public void testGetColumnPadding() {
        String tablename = "R-maxdata-CLINIDATA_NEW-DTW_PATIENT";
        String family = "DQE";
        String qualifier = "3";

        testSchemaParsing();

        TableSchema ts = parser.getTableSchema(tablename);
        System.out.println("Key Padding: "+ts.getKeyPadding());
        System.out.println("Column Padding: "+ts.getColumnPadding(family.getBytes(), qualifier.getBytes()));

    }



}
