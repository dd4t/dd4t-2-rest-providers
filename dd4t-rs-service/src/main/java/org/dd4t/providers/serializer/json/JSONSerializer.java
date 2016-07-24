package org.dd4t.providers.serializer.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.dd4t.core.exceptions.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class for serializing and deserializing objects using GZip, Base64 and JSON.
 *
 * @author Mihai Cadariu
 * @since 04.06.2014
 */
public class JSONSerializer {

    private final static Logger LOG = LoggerFactory.getLogger(JSONSerializer.class);

    /**
     * Jackson's 2.3.3 ObjectMapper
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Add JodaTime Serialization
    {
        MAPPER.registerModule(new JodaModule());
        MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Serializes an object to JSON representation. The object must be annotated with flexjson.
     *
     * @param object the object to serialize to JSON
     * @return String representing the JSON encoded object
     */
    public <T> String serializeJSON(T object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            LOG.error("Error while serializing object", e);
        }

        return null;
    }

    /**
     * Deserializes a JSON string into an object of a given type.
     *
     * @param json the String representing JSON
     * @param <T>  the class type to deserialized into
     * @return an objec ot given class type containing the JSON values
     */
    public <T> T deserializeJSON(String json, Class<T> aClass) {
        try {
            return MAPPER.readValue(json, aClass);
        } catch (IOException e) {
            LOG.error("Error while deserializing message", e);
        }

        return null;
    }

    /**
     * Utility method that checks whether the message pased as argument is in Base64 encoding or not.
     *
     * @param message String representing the message to check
     * @return boolean true if the message is in Base64 encoding; or false, otherwise
     */
    public boolean isBase64(String message) {
        return Base64.isBase64(message);
    }

    /**
     * Encodes the given byte array to Base64.
     *
     * @param byteArray the byte array to encode
     * @return String representing the encoded array
     */
    public String encodeBase64(byte[] byteArray) {
        if (!Base64.isBase64(byteArray)) {
            return Base64.encodeBase64String(byteArray);
        }

        return new String(byteArray, Charset.forName("UTF-8"));
    }

    /**
     * Decodes the given string using Base64 algorithm.
     *
     * @param message String representing the message to decode
     * @return byte[] representing the decoded array
     */
    public byte[] decodeBase64(String message) {
        if (Base64.isBase64(message)) {
            return Base64.decodeBase64(message);
        }

        byte[] result;
        try {
            result = message.getBytes("UTF-8");
        } catch (UnsupportedEncodingException uee) {
            result = message.getBytes();
        }

        return result;
    }

    /**
     * Compresses a given object to a GZipped byte array.
     *
     * @param object the object to encode
     * @return byte[] representing the compressed object bytes
     * @throws SerializationException if something goes wrong with the streams
     */
    public <T> byte[] compressGZipGeneric(T object) throws SerializationException {
        ByteArrayOutputStream baos = null;
        GZIPOutputStream gos = null;
        ObjectOutputStream oos = null;

        try {
            baos = new ByteArrayOutputStream();
            gos = new GZIPOutputStream(baos);
            oos = new ObjectOutputStream(gos);

            oos.writeObject(object);
            gos.close();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new SerializationException("Failed to compres object", ioe);
        } finally {
            IOUtils.closeQuietly(oos);
            IOUtils.closeQuietly(gos);
            IOUtils.closeQuietly(baos);
        }
    }

    /**
     * Compresses a given content to a GZipped byte array.
     *
     * @param content the content to encode
     * @return byte[] representing the compressed content bytes
     * @throws SerializationException if something goes wrong with the streams
     */
    public byte[] compressGZip(String content) throws SerializationException {
        ByteArrayOutputStream baos = null;
        GZIPOutputStream gos = null;

        try {
            baos = new ByteArrayOutputStream();
            gos = new GZIPOutputStream(baos);

            gos.write(content.getBytes());
            gos.close();

            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new SerializationException("Failed to compress String", ioe);
        } finally {
            IOUtils.closeQuietly(gos);
            IOUtils.closeQuietly(baos);
        }
    }

    /**
     * Dcompresses a byte array representing a GZip-compressed object into an object of the given class type.
     *
     * @param bytes the byte array to decompress
     * @param <T>   the class type to deserialize the byte array into
     * @return the deserialized object of the given class type
     * @throws SerializationException if something goes wrong with the streams
     */
    public <T> T decompressGZipGeneric(byte[] bytes) throws SerializationException {
        T result = null;
        ByteArrayInputStream bais = null;
        GZIPInputStream gis = null;
        ObjectInputStream ois = null;

        try {
            bais = new ByteArrayInputStream(bytes);
            gis = new GZIPInputStream(bais);
            ois = new ObjectInputStream(gis);

            result = (T) ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            throw new SerializationException("Object failed decompression", e);
        } finally {
            IOUtils.closeQuietly(ois);
            IOUtils.closeQuietly(gis);
            IOUtils.closeQuietly(bais);
        }

        return result;
    }

    /**
     * Dcompresses a byte array representing a GZip-compressed string back into a String.
     *
     * @param bytes the byte array to decompress
     * @return the deserialized object of the given class type
     * @throws SerializationException if something goes wrong with the streams
     */
    public String decompressGZip(byte[] bytes) throws SerializationException {
        String result = null;
        ByteArrayInputStream bais = null;
        GZIPInputStream gis = null;

        try {
            bais = new ByteArrayInputStream(bytes);
            gis = new GZIPInputStream(bais);

            result = (String) IOUtils.toString(gis);
        } catch (IOException ioe) {
            throw new SerializationException("Failed to decompress byte array", ioe);
        } finally {
            IOUtils.closeQuietly(gis);
            IOUtils.closeQuietly(bais);
        }

        return result;
    }

    public String prettyPrintJSON(String json) {
        if (json == null) {
            return "";
        }

        try {
            Object jsonObject = MAPPER.readValue(json, Object.class);
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        } catch (IOException e) {
            LOG.error("Cannot pretty print JSON", e);
        }

        return json;
    }

    public String prettyPrint(String encodedJSON) {
        if (encodedJSON == null) {
            return "";
        }

        try {
            byte[] bytes = decodeBase64(encodedJSON);
            String json = decompressGZip(bytes);
            return prettyPrintJSON(json);
        } catch (SerializationException e) {
            LOG.error("SerializationException occurred", e);
        }

        return encodedJSON;
    }

    public String prettyPrintDcps(String encodedJSON) {
        if (encodedJSON == null) {
            return "";
        }

        try {
            byte[] bytes = decodeBase64(encodedJSON);
            String json = new String(bytes,"UTF-8");
            return prettyPrintJSON(json);
        } catch (UnsupportedEncodingException e) {
            LOG.error("SerializationException occurred", e);
        }

        return encodedJSON;
    }
}
