package org.dd4t.providers.rs;

import com.tridion.broker.StorageException;
import com.tridion.broker.querying.Query;
import com.tridion.broker.querying.criteria.Criteria;
import com.tridion.broker.querying.criteria.content.ItemTypeCriteria;
import com.tridion.broker.querying.criteria.operators.AndCriteria;
import com.tridion.broker.querying.criteria.publication.PublicationMultimediaURLCriteria;
import com.tridion.broker.querying.filter.LimitFilter;
import com.tridion.broker.querying.sorting.SortDirection;
import com.tridion.broker.querying.sorting.SortParameter;
import com.tridion.storage.BinaryVariant;
import org.dd4t.contentmodel.Binary;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.core.caching.CacheType;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.core.exceptions.SerializationException;
import org.dd4t.core.util.TCMURI;
import org.dd4t.providers.impl.BrokerBinaryProvider;
import org.dd4t.providers.serializer.BinaryBuilder;
import org.dd4t.providers.serializer.SerializerFactory;
import org.dd4t.providers.serializer.json.JSONSerializer;
import org.dd4t.providers.transport.BinaryWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

/**
 * Tridion provider for binaries stored in the Content Delivery Database. The class retrieves either Binary metadata
 * (in form of @see BinaryVariant) or actual binary content (as byte[]).
 * <p/>
 * The returned items are also placed in an EHCache for future reads increased performance.
 *
 * @author Mihai Cadariu
 * @since 04.06.14
 */
public class TridionBinaryProvider extends TridionBaseProvider {

    private final static Logger LOG = LoggerFactory.getLogger(TridionBinaryProvider.class);
    private final BrokerBinaryProvider binaryProvider = new BrokerBinaryProvider();
    private static final TridionBinaryProvider INSTANCE = new TridionBinaryProvider();


    private TridionBinaryProvider() {

    }

    public static TridionBinaryProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerBinaryProvider#getBinaryContentById
     *
     * @param id          int representing the item id
     * @param publication int representing the publication id
     * @return byte[] the byte array of the binary content
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by id and publication was not found
     */
    public byte[] getBinaryContentById(final int id, final int publication)
            throws StorageException, ItemNotFoundException {
        LOG.debug("Fetching binary content id: {} and publication: {}", id, publication);

        String key = getKey(CacheType.BINARY_CONTENT, id, publication);
        CacheElement<byte[]> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        byte[] result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        result = binaryProvider.getBinaryContentById(id, publication);
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, publication, id);
                    } catch (ItemNotFoundException e) {
                        result = null;
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement, publication, id);
                        cacheElement.setExpired(true);
                    }
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException("Unable to find binary content by id '" + id + "' and publication '" + publication + "'.");
        }

        return result;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerBinaryProvider#getBinaryContentByURL
     * <p/>
     * The returned byte array is placed in Ehcache for faster future retrieval
     *
     * @param url         string representing the path portion of the URL of the binary
     * @param publication int representing the publication id
     * @return byte[] the byte array of the binary content
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by url and publication was not found
     */
    public byte[] getBinaryContentByURL(final String url, final int publication)
            throws StorageException, ItemNotFoundException {
        LOG.debug("Fetching binary content by url: {}, and publication: {}", url, publication);

        String decodedUrl = decodeUrl(url);
        String key = getKey(CacheType.BINARY_CONTENT, decodedUrl, publication);
        CacheElement<byte[]> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        byte[] result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        BinaryVariant variant = binaryProvider.getBinaryVariantByURL(decodedUrl, publication);
                        if (variant == null) {
                            result = null;
                            cacheElement.setPayload(null);
                            cacheProvider.storeInItemCache(key, cacheElement);
                        } else {
                            result = binaryProvider.getBinaryContentByURL(decodedUrl, publication);
                            cacheElement.setPayload(result);
                            cacheProvider.storeInItemCache(key, cacheElement, publication, variant.getBinaryId());
                        }
                    } catch (ItemNotFoundException e) {
                        result = null;
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                    }
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException("Unable to find binary content by url '" + url + "' and publication '" +
                    publication + "'.");
        }

        return result;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerBinaryProvider#getBinaryVariantById
     *
     * @param id          int representing the item id
     * @param publication int representing the publication id
     * @return String the encoded binary meta identified by id and publication
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by id and publication was not found
     */
    public String getBinaryMetaById(int id, int publication)
            throws StorageException, ItemNotFoundException, IOException {
        LOG.debug("Fetching binary meta id: {} and publication: {}", id, publication);

        String key = getKey(CacheType.BINARY_META, id, publication);
        CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        String result = null;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        BinaryVariant variant = binaryProvider.getBinaryVariantById(id, publication);
                        if (variant != null) {
                            result = serialize(variant);
                        }
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, publication, id);
                    } catch (ItemNotFoundException e) {
                        result = null;
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                    } catch (SerializationException se) {
                        throw new IOException(se);
                    }
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException("Unable to find binary meta by id '" + id + "' and publication '" +
                    publication + "'.");
        }

        return result;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerBinaryProvider#getBinaryMetaByURL
     *
     * @param url         string representing the path portion of the URL of the binary
     * @param publication int representing the publication id
     * @return String the encoded binary meta identified by publication id and URL
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by url and publication was not found
     */
    public String getBinaryMetaByURL(String url, int publication)
            throws StorageException, ItemNotFoundException, IOException {
        LOG.debug("Fetching binary meta by url: {} and publication: {}", url, publication);

        String decodedUrl = decodeUrl(url);
        String key = getKey(CacheType.BINARY_META, decodedUrl, publication);
        CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        String result = null;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        BinaryVariant variant = binaryProvider.getBinaryVariantByURL(decodedUrl, publication);
                        if (variant == null) {
                            cacheElement.setPayload(null);
                            cacheProvider.storeInItemCache(key, cacheElement);
                            cacheElement.setExpired(true);
                        } else {
                            result = serialize(variant);
                            cacheElement.setPayload(result);
                            cacheProvider.storeInItemCache(key, cacheElement, publication, variant.getBinaryId());
                        }
                    } catch (ItemNotFoundException e) {
                        result = null;
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                    } catch (SerializationException se) {
                        throw new IOException(se);
                    }
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException("Unable to find binary meta by url '" + url + "' and publication '" +
                    publication + "'.");
        }

        return result;
    }

    /**
     * Retrieves binary meta and binary content for a given Tridion Binary published to the CD DB. The binary meta is
     * placed inside a BinaryImpl object that is JSONed, then GZipped and then Base64 encoded. The binary content is
     * a byte array. They are both placed in a BinaryWrapper object that is GZip encoded to a byte array.
     *
     * @param id          int representing the item id
     * @param publication int representing the publication id
     * @return byte[] the byte array GZipped representation of the BinaryWrapper object
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by url and publication was not found
     * @throws IOException           if something went wrong during compressing the BinaryWrapper
     */
    public byte[] getWrapperById(int id, int publication)
            throws StorageException, ItemNotFoundException, IOException {
        LOG.debug("Fetching binary wrapper id: {} and publication: {}", id, publication);

        try {
            byte[] content = getBinaryContentById(id, publication);
            String binary = getBinaryMetaById(id, publication);
            BinaryWrapper wrapper = new BinaryWrapper(binary, content);

            JSONSerializer serializer = SerializerFactory.getSerializer();
            return serializer.compressGZipGeneric(wrapper);
        } catch (SerializationException se) {
            throw new IOException(se);
        }
    }

    /**
     * Retrieves binary meta and binary content for a given Tridion Binary published to the CD DB. The binary meta is
     * placed inside a BinaryImpl object that is JSONed, then GZipped and then Base64 encoded. The binary content is
     * a byte array. They are both placed in a BinaryWrapper object that is GZip encoded to a byte array.
     *
     * @param url         string representing the path portion of the URL of the binary
     * @param publication int representing the publication id
     * @return byte[] the byte array GZipped representation of the BinaryWrapper object
     * @throws StorageException      if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by url and publication was not found
     * @throws IOException           if something went wrong during compressing the BinaryWrapper
     */
    public byte[] getWrapperByURL(String url, int publication) throws
            StorageException, ItemNotFoundException, IOException {
        LOG.debug("Fetching binary wrapper url: {} and publication: {}", url, publication);

        try {
            byte[] content = getBinaryContentByURL(url, publication);
            String binary = getBinaryMetaByURL(url, publication);
            BinaryWrapper wrapper = new BinaryWrapper(binary, content);

            JSONSerializer serializer = SerializerFactory.getSerializer();
            return serializer.compressGZipGeneric(wrapper);
        } catch (SerializationException se) {
            throw new IOException(se);
        }
    }

    /**
     * Returns the Publication TCMURI item id corresponding to the given Images URL stub
     *
     * @param imagesUrl String representing the Images URL metadata as defined in Tridion
     * @return int representing the Publication id if found; or 0, otherwise
     * @throws ItemNotFoundException if the requested URL stubs is null
     * @throws ParseException        if the retrieved item TCMURI is not well formed
     * @throws StorageException      if something went wrong during accessing the CD DB
     */
    public int discoverPublicationId(final String imagesUrl)
            throws ItemNotFoundException, ParseException, StorageException {
        LOG.debug("Discovering Publication id for imagesUrl: {}", imagesUrl);

        String decodedUrl = decodeUrl(imagesUrl);
        String key = getKey(CacheType.DISCOVER_IMAGES_URL, decodedUrl);
        CacheElement<Integer> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        Integer result = null;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);

                    Criteria publicationCriteria = new PublicationMultimediaURLCriteria(decodedUrl);
                    Criteria componentTemplateCriteria = new ItemTypeCriteria(32);
                    Criteria criteria = new AndCriteria(componentTemplateCriteria, publicationCriteria);

                    Query query = new Query(criteria);
                    query.addSorting(new SortParameter(SortParameter.ITEMS_LAST_PUBLISHED_DATE, SortDirection.DESCENDING));

                    String[] results = query.executeQuery();
                    if (results == null || results.length == 0) {
                        Criteria pageCriteria = new ItemTypeCriteria(64);
                        criteria = new AndCriteria(pageCriteria, publicationCriteria);

                        query = new Query(criteria);
                        query.setResultFilter(new LimitFilter(1));
                        query.addSorting(new SortParameter(SortParameter.ITEMS_LAST_PUBLISHED_DATE, SortDirection.DESCENDING));
                        results = query.executeQuery();
                    }

                    if (results != null && results.length > 0) {
                        if (containsDuplicates(results)) {
                            LOG.error("Found duplicate Publication IDs. Returning id: -1.");
                            result = -1;
                        } else {
                            TCMURI tcmUri = new TCMURI(results[0]);
                            result = tcmUri.getPublicationId();
                            LOG.debug("Discovered Publication id {}", result);
                        }
                    }

                    cacheElement.setPayload(result);
                    cacheProvider.storeInItemCache(key, cacheElement);
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        return result == null ? 0 : result;
    }

    private boolean containsDuplicates(String[] tcmuris) throws ParseException {
        if (tcmuris != null && tcmuris.length > 0) {
            TCMURI tcmUri = new TCMURI(tcmuris[0]);
            int publicationId = tcmUri.getPublicationId();

            for (String uri : tcmuris) {
                tcmUri = new TCMURI(uri);
                if (publicationId != tcmUri.getPublicationId()) {
                    LOG.error("Found duplicate Publications with the same 'PublicationUrl': " + publicationId + " and "
                            + tcmUri.getPublicationId());
                    return true;
                }
            }
        }

        return false;
    }

    private String decodeUrl(final String url) throws ItemNotFoundException {
        if (null != url) {
            String decodedUrl = new String(urlCoder.decode(url));
            LOG.debug("Decoded Url: {} ", decodedUrl);
            return decodedUrl;
        }

        throw new ItemNotFoundException("Url parameter could not be decoded. Item not found or parameter was null.");
    }

    /*
     * Serializes the given @see BinaryVariant to a JSON encoded string. The method performs the following:
     * a) mapping BinaryVariant to @see org.dd4t.contentmodel.Binary without BinaryData, b) serialization of
     * Binary to JSON, c) compression JSON with GZIP, and d) encode JSON to Base64
     */
    private String serialize(BinaryVariant variant) throws SerializationException {
        BinaryBuilder builder = new BinaryBuilder();
        Binary binary = builder.build(variant);

        JSONSerializer serializer = SerializerFactory.getSerializer();
        String json = serializer.serializeJSON(binary);
        byte[] gzipArray = serializer.compressGZip(json);

        return serializer.encodeBase64(gzipArray);
    }

    /**
     * Builds a key using a named cache type (region), a URL and a Publication id. This type of key is used to point to
     * the actual payload in the cache. Use this key when looking up objects cached for a particular URL.
     *
     * @param type        CacheType representing the type (or region) where the associated item is in cache
     * @param url         the path part of the URL of a Tridion item
     * @param publication the publication id of the cache item
     * @return String representing the key pointing to a URL value
     */
    private String getKey(CacheType type, String url, int publication) {
        return String.format("%s-%s-%d", type, url, publication);
    }

    /**
     * Builds a key using a named cache type (region), an item id and a Publication id. This type of key is used to
     * point to a URL. Use this key when invalidating cache items (e.g. from JMS listener) and you only know the TcmUri.
     *
     * @param type        CacheType representing the type (or region) where the associated item is in cache
     * @param id          the Tridion TcmUri item id of the cache item
     * @param publication the publication id of the cache item
     * @return String representing the key pointing to a URL value
     */
    private String getKey(CacheType type, int id, int publication) {
        return String.format("%s-%d-%d", type, publication, id);
    }

    /**
     * Builds a key using a named cache type (region) and a URL. This type of key is used to point to
     * actual payload in the cache. Use this key when looking up objects cached for a particular URL.
     *
     * @param type CacheType representing the type (or region) where the associated item is in cache
     * @param url  the path part of the URL of a Tridion item
     * @return String representing the key pointing to a URL value
     */
    private String getKey(CacheType type, String url) {
            return String.format("%s-%s", type, url);
    }
}
