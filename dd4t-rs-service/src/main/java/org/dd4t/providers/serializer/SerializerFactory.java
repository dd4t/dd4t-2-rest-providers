package org.dd4t.providers.serializer;

import org.dd4t.providers.serializer.json.JSONSerializer;

/**
 * @author Mihai Cadariu
 * @since 04.07.2014
 */
public class SerializerFactory {

    private static final SerializerFactory INSTANCE = new SerializerFactory();
    private JSONSerializer serializer;

    private SerializerFactory() {
        setCacheProvider(new JSONSerializer());
    }

    public static JSONSerializer getSerializer() {
        return INSTANCE.serializer;
    }

    public void setCacheProvider(JSONSerializer provider) {
        serializer = provider;
    }
}
