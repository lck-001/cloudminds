package com.cloudminds.cdc.deserialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.charset.Charset;

public class CustomerCharsetSerializer extends Serializer {
    @Override
    public void write(Kryo kryo, Output output, Object object) {
        Charset charset = (Charset) object;
        kryo.writeObject(output, charset.name());
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        return Charset.forName(kryo.readObject(input, String.class));
    }
}
