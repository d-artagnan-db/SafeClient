package pt.uminho.haslab.safeclient.decoders;


import org.apache.derby.iapi.services.io.FormatIdInputStream;
import org.apache.derby.iapi.services.io.FormatIdOutputStream;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.store.tuplestore.util.CloneableByteArrayInputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;

public abstract class DerbyEncoderDecoder implements Decoder, Encoder {


    protected FormatIdInputStream getInputStream(byte[] value) {
        return new FormatIdInputStream(new CloneableByteArrayInputStream(value));
    }

    protected byte[] encodeDVD(DataValueDescriptor dvd) throws IOException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        ObjectOutput out = new FormatIdOutputStream(bOut);
        dvd.writeExternal(out);
        out.flush();
        out.close();
        return bOut.toByteArray();
    }

}
