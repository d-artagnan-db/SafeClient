package pt.uminho.haslab.safeclient.helpers;

import pt.uminho.haslab.smpc.interfaces.Player;

import java.math.BigInteger;
import java.util.List;

public class TestPlayer implements Player{
    @Override
    public void sendValueToPlayer(int i, BigInteger bigInteger) {

    }

    @Override
    public void storeValue(Integer integer, Integer integer1, BigInteger bigInteger) {

    }

    @Override
    public void storeValues(Integer integer, Integer integer1, List<byte[]> list) {

    }

    @Override
    public void storeValues(Integer integer, Integer integer1, int[] ints) {

    }

    @Override
    public void storeValues(Integer integer, Integer integer1, long[] longs) {

    }

    @Override
    public BigInteger getValue(Integer integer) {
        return null;
    }

    @Override
    public int getPlayerID() {
        return 0;
    }

    @Override
    public void sendValueToPlayer(Integer integer, List<byte[]> list) {

    }

    @Override
    public List<byte[]> getValues(Integer integer) {
        return null;
    }

    @Override
    public void sendValueToPlayer(Integer integer, int[] ints) {

    }

    @Override
    public void sendValueToPlayer(Integer integer, long[] longs) {

    }

    @Override
    public int[] getIntValues(Integer integer) {
        return new int[0];
    }

    @Override
    public long[] getLongValues(Integer integer) {
        return new long[0];
    }
}
