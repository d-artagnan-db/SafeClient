package pt.uminho.haslab.safeclient.decoders;

import pt.uminho.haslab.safemapper.TableSchema;


public class DecodingFactory {


    private static Object getEncoderDecoder(TableSchema schema, String family, String qualifier) {
        String type = schema.getFamily(family).getQualifier(qualifier).getProperties().get("encoding");
        EncodingType dectype = EncodingType.valueOf(type);
        switch (dectype) {
            case SQLDecimal:
                return SQLDecimalEncoderDecoder.createSQLDecimalEncoder();
            case DerbyString:
                return DerbyStringEncoderDecoder.createDerbyStringDecoder();
            default:
                return ByteBufferDecoder.createByteBufferDecoder();
        }
    }

    public static Decoder decoder(TableSchema schema, String family, String qualifier) {
        return (Decoder) getEncoderDecoder(schema, family, qualifier);
    }

    public static Encoder encoder(TableSchema schema, String family, String qualifier) {
        return (Encoder) getEncoderDecoder(schema, family, qualifier);
    }


}
