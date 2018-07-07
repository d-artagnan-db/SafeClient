package pt.uminho.haslab.safeclient.decoders;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;


public interface Decoder {

    int getInt(byte[] value) throws StandardException, IOException;

    long getLong(byte[] value) throws StandardException, IOException;

    byte[] getStringArray(byte[] value);
}
