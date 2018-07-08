package pt.uminho.haslab.safeclient.decoders;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

public class DerbyStringEncoderDecoder implements Encoder, Decoder {
    private static DerbyStringEncoderDecoder encoder;

    public synchronized static DerbyStringEncoderDecoder createDerbyStringDecoder() {
        if (encoder == null) {
            encoder = new DerbyStringEncoderDecoder();
        }

        return encoder;
    }

    @Override
    public int getInt(byte[] value) throws StandardException, IOException {
        throw new IllegalStateException("GetInt not supported on  DerbyStringEncoderDecoder");
    }

    @Override
    public long getLong(byte[] value) throws StandardException, IOException {
        throw new IllegalStateException("GetLong not supported on  DerbyStringEncoderDecoder");
    }


    @Override
    public byte[] encodeLong(long value) throws IOException {
        throw new IllegalStateException("encodeLong not supported on  DerbyStringEncoderDecoder");
    }

    @Override
    public byte[] encodeInt(int value) throws IOException {
        throw new IllegalStateException("encodeInt not supported on  DerbyStringEncoderDecoder");
    }

    @Override
    public byte[] encodeString(byte[] str) {
        return str;
    }

    @Override
    public byte[] getStringArray(byte[] value) {
        return value;

    }
}
