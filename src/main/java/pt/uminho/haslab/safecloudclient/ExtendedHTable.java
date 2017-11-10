package pt.uminho.haslab.safecloudclient;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

public interface ExtendedHTable extends HTableInterface {


    HRegionLocation getRegionLocation(byte[] row) throws IOException;


}
