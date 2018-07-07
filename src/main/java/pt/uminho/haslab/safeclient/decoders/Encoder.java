package pt.uminho.haslab.safeclient.decoders;

import java.io.IOException;

public interface Encoder {

    byte[] encodeLong(long value) throws IOException;
    byte[] encodeInt(int value) throws IOException;
    byte[] encodeString(byte[] str);
}
