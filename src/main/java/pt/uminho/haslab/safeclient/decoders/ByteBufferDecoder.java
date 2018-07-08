package pt.uminho.haslab.safeclient.decoders;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ByteBufferDecoder implements Encoder, Decoder {

    private static ByteBufferDecoder encoder;

    public synchronized static ByteBufferDecoder createByteBufferDecoder() {
        if (encoder == null) {
            encoder = new ByteBufferDecoder();
        }

        return encoder;
    }

    @Override
    public int getInt(byte[] value) {
        return ByteBuffer.wrap(value).getInt();
    }

    @Override
    public long getLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong();
    }


    @Override
    public byte[] encodeLong(long value) throws IOException {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] encodeInt(int value) throws IOException {
        return Bytes.toBytes(value);
    }

    @Override
    public byte[] getStringArray(byte[] value) {
        throw new UnsupportedOperationException("getStringArray not supported on ByteBufferDecoder");
    }

    @Override
    public byte[] encodeString(byte[] str) {
        throw new UnsupportedOperationException("encodeString not supported on ByteBufferDecoder");
    }
}
