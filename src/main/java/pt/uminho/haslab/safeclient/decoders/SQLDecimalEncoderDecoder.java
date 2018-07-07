package pt.uminho.haslab.safeclient.decoders;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatIdInputStream;
import org.apache.derby.iapi.types.SQLDecimal;

import java.io.IOException;
import java.math.BigDecimal;

public class SQLDecimalEncoderDecoder extends DerbyEncoderDecoder {


    private static SQLDecimalEncoderDecoder encoder;

    public synchronized static SQLDecimalEncoderDecoder createSQLDecimalEncoder(){
        if(encoder == null){
            encoder = new SQLDecimalEncoderDecoder();
        }
        return encoder;
    }

    private SQLDecimal getDecodeDecimal(byte[] val) throws IOException {
        SQLDecimal decimal = new SQLDecimal();
        FormatIdInputStream stream = getInputStream(val);
        decimal.readExternal(stream);
        return decimal;
    }

    @Override
    public int getInt(byte[] val) throws StandardException, IOException {
        return getDecodeDecimal(val).getInt();
    }

    @Override
    public long getLong(byte[] val) throws StandardException, IOException {
       return getDecodeDecimal(val).getLong();
    }


    @Override
    public byte[] encodeLong(long value) throws IOException {
        SQLDecimal decimal = new SQLDecimal(BigDecimal.valueOf(value));
        return encodeDVD(decimal);
    }

    @Override
    public byte[] encodeInt(int value) throws IOException {
        SQLDecimal decimal = new SQLDecimal(BigDecimal.valueOf(value));
        return encodeDVD(decimal);
    }

    @Override
    public byte[] getStringArray(byte[] value) {
        throw new IllegalStateException("getStringArray not supported on SQLDecimalEncoderDecoder");
    }

    @Override
    public byte[] encodeString(byte[] str) {
        throw new IllegalStateException("encodeString not supported on SQLDecimalEncoderDecoder");
    }
}
