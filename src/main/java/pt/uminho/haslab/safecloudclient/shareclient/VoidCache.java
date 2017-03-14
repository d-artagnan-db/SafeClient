package pt.uminho.haslab.safecloudclient.shareclient;

/**
 *
 * Void cache that does not store any value.
 * It is used to replace the standard cache implementation without modifying
 * the code flow and measure the impact the caches has.
 */
public class VoidCache implements ClientCache{

    public void put(String tableName, byte[] row, Long uniqueID) {
    }

    public Long get(String tableName, byte[] row) {
        return null;
    }

    public boolean containsKey(String tableName, byte[] key) {
        return false;
    }
    
}
