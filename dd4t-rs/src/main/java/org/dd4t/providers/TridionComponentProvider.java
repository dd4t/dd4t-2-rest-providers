package org.dd4t.providers;

import com.tridion.broker.StorageException;
import org.dd4t.contentmodel.exceptions.ItemNotFoundException;
import org.dd4t.contentmodel.exceptions.SerializationException;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.providers.caching.CacheProviderFactory;
import org.dd4t.providers.caching.CacheType;
import org.dd4t.providers.impl.BrokerComponentProvider;
import org.dd4t.providers.serializer.SerializerFactory;
import org.dd4t.providers.serializer.json.JSONSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

/**
 * Provides access to Dynamic Component Presentation content and stored in the Tridion Content Delivery database.
 * This provider also handles serialization and compression of DCP content.
 * <p/>
 * The content is stored in and retrieved from a EHCache instance.
 *
 * @author Mihai Cadariu
 * @since 20.06.14
 */
public enum TridionComponentProvider implements ComponentProvider {

    INSTANCE;
    private final static Logger LOG = LoggerFactory.getLogger(TridionComponentProvider.class);
    private final BrokerComponentProvider componentProvider = new BrokerComponentProvider();
    private final CacheProvider cacheProvider;

    private TridionComponentProvider() {
        cacheProvider = CacheProviderFactory.getCacheProvider();
    }

    /**
     * Retrieves content of a Dynamic Component Presentation by looking up its componentId and publicationId.
     * A templateId is not provided, so the DCP with the highest linking priority is retrieved.
     * The returned content represents a JSON encoded string, which is then GZip compressed and finally Base64 encoded.
     * <p/>
     * <b>Note: This method performs significantly slower than getDynamicComponentPresentation(int, int, int)!
     * Do provide a templateId!</b>
     *
     * @param componentId   int representing the Component item id
     * @param publicationId int representing the Publication id of the DCP
     * @return String representing the content of the DCP
     * @throws IOException           if something went wrong during serialization
     * @throws ItemNotFoundException if the requested DCP does not exist
     */
    @Override
    public String getDynamicComponentPresentation(int componentId, int publicationId)
            throws IOException, ItemNotFoundException {
        return getDynamicComponentPresentation(componentId, 0, publicationId);
    }

    /**
     * Retrieves content of a Dynamic Component Presentation by looking up its componentId, templateId and publicationId.
     * The returned content represents a JSON encoded string, which is then GZip compressed and finally Base64 encoded.
     *
     * @param componentId   int representing the Component item id
     * @param templateId    int representing the Component Template item id
     * @param publicationId int representing the Publication id of the DCP
     * @return String representing the content of the DCP
     * @throws IOException           if something went wrong during serialization
     * @throws ItemNotFoundException if the requested DCP does not exist
     */
    @Override
    public String getDynamicComponentPresentation(int componentId, int templateId, int publicationId)
            throws IOException, ItemNotFoundException {
        Object[] logParameters = {componentId, templateId, publicationId};
        LOG.debug("Fetching Component Presentation by componentId: {}, templateId: {} and publicationId: {}", logParameters);

        String key = getKey(CacheType.ComponentContent, componentId, templateId, publicationId);
        CacheElement<String> cacheElement = cacheProvider.loadFromLocalCache(key);
        String result = null;

        if (cacheElement.isExpired()) {
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        long start = System.currentTimeMillis();

                        result = componentProvider.getDynamicComponentPresentation(componentId, templateId, publicationId);
                        LOG.debug("GET COMPONENT: {} ms.",(System.currentTimeMillis()-start));


                        result = serialize(result);
                        LOG.debug("Serialize: {} ms.",(System.currentTimeMillis()-start));
                        LOG.debug("Result: {}", result);

                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, publicationId, componentId);
                        LOG.debug("Added DCP to cache with componentId: {}, templateId: {} and publicationId: {}",
                                logParameters);
                    } catch (ItemNotFoundException e) {
                        LOG.info("Could not find DCP in broker DB. Caching null payload for componentId: {}, templateId: {} and publicationId: {}",
                                logParameters);
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                    } catch (SerializationException se) {
                        LOG.error(String.format("Could not serialize DCP with componentId: %d, templateId: %d and publicationId: %d",
                                logParameters), se);
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                        throw new IOException(se);
                    }
                } else {
                    LOG.debug("Serving DCP from cache with componentId: {}, templateId: {} and publicationId: {}",
                            logParameters);
                    result = cacheElement.getPayload();
                }
            }
        } else {
            LOG.debug("Serving DCP from cache with componentId: {}, templateId: {} and publicationId: {}",
                    logParameters);
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException(String.format("Component Presentation not found for componentId: %d, templateId: %d and publicationId: %d",
                    componentId, templateId, publicationId));
        }

        return result;
    }

    public List<com.tridion.storage.ComponentPresentation> getComponentPresentations(String[] tcmUris, int templateId, int publicationId) throws StorageException, ParseException {
        return componentProvider.getComponentPresentations(tcmUris,templateId,publicationId);
    }

    /**
     * Compresses the given DCP content, then encodes it to Base64. Content is already supposed to be in JSON format.
     *
     * @param content String the DCP content to serialize
     * @return String the compressed content in Base64 encoding
     */
    private String serialize(String content) throws SerializationException {
        JSONSerializer serializer = SerializerFactory.getSerializer();

        if (content != null && !serializer.isBase64(content)) {
            byte[] gzipArray = serializer.compressGZipGeneric(content);
            return serializer.encodeBase64(gzipArray);
        } else {
            return content;
        }
    }

    /**
     * Builds a key using a named cache type (region) and three ids. This type of key is used to point to
     * the actual payload in the cache (e.g. cached Component Presentations).
     *
     * @param type CacheType representing the type (or region) where the associated item is in cache
     * @param id1  the Tridion TcmUri item id
     * @param id2  the Tridion TcmUri item id
     * @param id3  the Tridion TcmUri item id
     * @return String representing the key pointing to a cached value
     */
    private String getKey(CacheType type, int id1, int id2, int id3) {
        return String.format("%s-%d-%d-%d", type, id1, id2, id3);
    }
}
