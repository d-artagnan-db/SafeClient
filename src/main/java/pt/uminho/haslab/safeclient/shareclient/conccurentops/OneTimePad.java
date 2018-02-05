package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import java.security.SecureRandom;

class OneTimePad {
	static byte[][] oneTimePadEncode(byte[] value) {

	    byte[][] encValues = new byte[3][];

		SecureRandom random = new SecureRandom();
		byte[] firstRandom = new byte[value.length];
		byte[] secondRandom = new byte[value.length];
		byte[] encValue  = new byte[value.length];

		random.nextBytes(firstRandom);
		random.nextBytes(secondRandom);


		for(int i = 0; i < value.length; i++){
			encValue[i] = (byte) (firstRandom[i] ^ secondRandom[i] ^ value[i]);
		}
		encValues[0] = firstRandom;
		encValues[1] = secondRandom;
		encValues[2] = encValue;
		return encValues;
	}

	static byte[] oneTimeDecode(byte[][] values) {
		byte[] value = new byte[values[0].length];
		for(int i = 0; i < value.length; i++){
		    value[i] = (byte) (values[0][i] ^ values[1][i] ^ values[2][i]);
        }
        return value;
	}

}
