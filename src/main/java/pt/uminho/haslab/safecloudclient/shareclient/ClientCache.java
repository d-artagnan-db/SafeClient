package pt.uminho.haslab.safecloudclient.shareclient;



public interface ClientCache {

    public void put(String tableName, byte[] row, Long uniqueID);

    public Long get(String tableName, byte[] row);

    public boolean containsKey(String tableName, byte[] key);
}
