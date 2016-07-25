package org.dd4t.providers.rs;

import com.tridion.storage.PageMeta;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.core.caching.CacheType;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.core.exceptions.SerializationException;
import org.dd4t.core.util.TCMURI;
import org.dd4t.providers.PageProvider;
import org.dd4t.providers.ProviderResultItem;
import org.dd4t.providers.impl.BrokerPageProvider;
import org.dd4t.providers.serializer.SerializerFactory;
import org.dd4t.providers.serializer.json.JSONSerializer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

/**
 * @author Rai
 * @since 01/05/14.
 */

public class TridionPageProvider extends TridionBaseProvider implements PageProvider {
    private final BrokerPageProvider pageProvider = new BrokerPageProvider();
    private final static Logger LOG = LoggerFactory.getLogger(TridionPageProvider.class);

    private static final TridionPageProvider INSTANCE = new TridionPageProvider();

    private TridionPageProvider () {

    }

    public static TridionPageProvider getInstance () {
        return INSTANCE;
    }


    public PageMeta getPageMetaByURL (final String url, final int publication) throws ItemNotFoundException {
        // TODO: cache
        LOG.debug("Fetching Page Meta by Url: {} and publication: {}", url, publication);
        return pageProvider.getPageMetaByURL(decodeUrl(url), publication);
    }


    private String decodeUrl (final String url) throws ItemNotFoundException {
        if (null != url) {
            String decodedUrl = new String(urlCoder.decode(url));
            LOG.debug("Decoded Url: {} ", decodedUrl);
            return decodedUrl;
        }
        throw new ItemNotFoundException("Url parameter could not be decoded. Item not found or parameter was null.");
    }

    @Override
    public ProviderResultItem<String> getPageById (final int id, final int publication) throws IOException, ItemNotFoundException, SerializationException {
        return null;
    }

    @Override
    public ProviderResultItem<String> getPageByURL (final String url, final int publication) throws ItemNotFoundException, SerializationException {
        return null;
    }

    @Override
    public String getPageContentById (final int id, final int publication) throws IOException, ItemNotFoundException, SerializationException {
        return pageProvider.getPageContentById(id, publication);
    }

    @Override
    public String getPageContentByURL (final String url, final int publication) throws ItemNotFoundException, SerializationException {
        LOG.debug("Fetching Page Content by url: {} and publication: {}", url, publication);

        String decodedUrl = decodeUrl(url);
        String key = getKey(CacheType.PAGE_CONTENT, decodedUrl, publication);
        CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        String result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        final PageMeta pageMeta = getPageMetaByURL(url, publication);
                        if (pageMeta == null) {
                            result = null;
                            cacheElement.setPayload(null);
                            cacheProvider.storeInItemCache(key, cacheElement);
                            cacheElement.setExpired(true);
                        } else {
                            result = pageProvider.getPageContentById(pageMeta.getItemId(), publication);
                            result = serialize(result);

                            cacheElement.setPayload(result);
                            cacheProvider.storeInItemCache(key, cacheElement, publication, pageMeta.getItemId());
                            LOG.debug("Stored Page Content with key: {} in cache", key);
                        }
                    } catch (ItemNotFoundException | SerializationException e) {
                        LOG.info("Unable to find page by url '" + url + "' and publication '" + publication + "': " + e.getMessage());
                        result = null;
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);
                        cacheElement.setExpired(true);
                    }
                } else {
                    LOG.debug("Fetched a Page Content with key: {} from cache", key);
                    result = cacheElement.getPayload();
                }
            }
        } else {
            LOG.debug("Fetched Page Content with key: {} from cache", key);
            result = cacheElement.getPayload();
        }

        if (result == null) {
            throw new ItemNotFoundException("Unable to find page by url '" + url + "' and publication '" + publication + "'.");
        }

        return result;


    }

    @Override
    public String getPageContentById (final String tcmUri) throws ItemNotFoundException, ParseException, SerializationException {
        return pageProvider.getPageContentById(tcmUri);
    }

    @Override
    public String getPageListByPublicationId (final int publication) throws ItemNotFoundException, SerializationException {
        return pageProvider.getPageListByPublicationId(publication);
    }

    @Override
    public boolean checkPageExists (final String url, final int publicationId) throws ItemNotFoundException, SerializationException {
        return pageProvider.checkPageExists(url, publicationId);
    }

    @Override
    public TCMURI getPageIdForUrl (final String url, final int publicationId) throws ItemNotFoundException, SerializationException {
        return pageProvider.getPageIdForUrl(url, publicationId);
    }

    @Override
    public DateTime getLastPublishDate (final String url, final int publication) throws ItemNotFoundException {
        return pageProvider.getLastPublishDate(url, publication);
    }

    /**
     * Compresses the given page content, then encodes it to BASE64. Content is already supposed to be in JSON format.
     *
     * @param content String the page content to serialize
     * @return String the compressed content in BASE64 encoding
     */
    private String serialize (String content) throws SerializationException {
        JSONSerializer serializer = SerializerFactory.getSerializer();

        if (content != null && !serializer.isBase64(content)) {
            byte[] gzipArray = serializer.compressGZip(content);
            return serializer.encodeBase64(gzipArray);
        } else {
            return content;
        }
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
    private String getKey (CacheType type, String url, int publication) {
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
    private String getKey (CacheType type, int id, int publication) {
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
    private String getKey (CacheType type, String url) {
        return String.format("%s-%s", type, url);
    }

    /**
     * Builds a key using a named cache type (region) and a URL. This type of key is used to point to
     * actual payload in the cache. Use this key when looking up objects cached for a particular URL.
     *
     * @param type CacheType representing the type (or region) where the associated item is in cache
     * @param id   the path part of the URL of a Tridion item
     * @return String representing the key pointing to a URL value
     */
    private String getKey (CacheType type, int id) {
        return String.format("%s-%s", type, id);
    }
}
