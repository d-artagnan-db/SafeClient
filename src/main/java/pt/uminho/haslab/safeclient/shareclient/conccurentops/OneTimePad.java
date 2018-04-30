package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class OneTimePad {
    public static List<byte[]> oneTimePadEncode(byte[] value) {
        List<byte[]> encValues = new ArrayList<byte[]>();

        SecureRandom random = new SecureRandom();
        byte firstRandom[] = new byte[value.length];
        byte secondRandom[] = new byte[value.length];

        random.nextBytes(firstRandom);
        random.nextBytes(secondRandom);
        encValues.add(firstRandom);
        encValues.add(secondRandom);

        BigInteger bfRandom = new BigInteger(firstRandom);
        BigInteger bsRandom = new BigInteger(secondRandom);
        BigInteger bvRandom = new BigInteger(value);

        byte encValue[] = bfRandom.xor(bsRandom).xor(bvRandom).toByteArray();

        encValues.add(encValue);
        return encValues;
    }

    public static byte[] oneTimeDecode(List<byte[]> values) {
        BigInteger firstSecret = new BigInteger(values.get(0));
        BigInteger secondSecret = new BigInteger(values.get(1));
        BigInteger thirdSecret = new BigInteger(values.get(2));
        return firstSecret.xor(secondSecret).xor(thirdSecret).toByteArray();
    }

}
