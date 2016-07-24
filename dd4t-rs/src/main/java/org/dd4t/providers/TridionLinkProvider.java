package org.dd4t.providers;

import com.tridion.util.TCMURI;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.providers.caching.CacheProviderFactory;
import org.dd4t.providers.caching.CacheType;
import org.dd4t.providers.impl.BrokerLinkProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

/**
 * Tridion provider for Component links. The class resolves either stand-alone Component links or Componen links
 * a given Page.
 * <p/>
 * The returned items are placed in an EHCache for future reads increased performance.
 *
 * @author Mihai Cadariu
 * @since 09.06.2014
 */
public enum TridionLinkProvider implements LinkProvider {

    INSTANCE;
    private final static Logger LOG = LoggerFactory.getLogger(TridionLinkProvider.class);
    private final BrokerLinkProvider linkProvider = new BrokerLinkProvider();
    private final CacheProvider cacheProvider;

    private TridionLinkProvider() {
        cacheProvider = CacheProviderFactory.getCacheProvider();
    }

    /**
     * Returns a link URL to the given Component TcmUri, if exists. Otherwise, returns null. The value is stored in and
     * retrieved from a EHCache instance.
     *
     * @param targetComponentURI String representing the TcmUri of the Component to resolve a link to
     * @return String representing the URL of the link; or null, if the Component is not linked to
     * @throws ParseException if the TCMURI is invalid
     */
    @Override
    public String resolveComponent(String targetComponentURI) throws ParseException {
        LOG.debug("Fetching link to Component with tcmuri: {}", targetComponentURI);

        String key = getKey(CacheType.ComponentLink, targetComponentURI);
        CacheElement<String> cacheElement = cacheProvider.loadFromLocalCache(key);
        String result;

        if (cacheElement.isExpired()) {
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    result = linkProvider.resolveComponent(targetComponentURI);
                    cacheElement.setPayload(result);
                    TCMURI tcmUri = new TCMURI(targetComponentURI);
                    cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        return result;
    }

    /**
     * Returns a link URL to the given Component id, if exists. Otherwise, returns null. The value is stored in and
     * retrieved from a EHCache instance.
     *
     * @param id          int representing the Component item id to resolve a link to
     * @param publication int representing the context Publication id to resolve the link in
     * @return String representing the URL of the link; or null, if the Component is not linked to
     * @throws ParseException if the TCMURI is invalid
     */
    public String resolveComponentById(int id, int publication) throws ParseException {
        return resolveComponent(new TCMURI(publication, id, 16, 0).toString());
    }

    /**
     * Returns a link URL to the given Component TcmUri, when also specifying the source Page TcmUri. The value is
     * stored in and retrieved from a EHCache instance.
     *
     * @param targetComponentURI String representing the TcmUri of the Component to resolve a link to
     * @param sourcePageURI      String representing the TcmUri of the Page the link appears on
     * @return String representing the URL of the link; or null, if the Component is not linked to
     * @throws ParseException if the TCMURIs are invalid
     */
    @Override
    public String resolveComponentFromPage(String targetComponentURI, String sourcePageURI) throws ParseException {
        LOG.debug("Fetching link to Component: {} from Page: {}", targetComponentURI, sourcePageURI);

        String key = getKey(CacheType.ComponentLink, targetComponentURI, sourcePageURI);
        CacheElement<String> cacheElement = cacheProvider.loadFromLocalCache(key);
        String result;

        if (cacheElement.isExpired()) {
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    result = linkProvider.resolveComponentFromPage(targetComponentURI, sourcePageURI);
                    cacheElement.setPayload(result);

                    TCMURI tcmUri = new TCMURI(targetComponentURI);
                    cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        return result;
    }

    /**
     * Returns a link URL to the given Component id, when also specifying the source Page id and Publication id. The
     * value is stored in and retrieved from a EHCache instance.
     *
     * @param componentId   int representing the Component item id to to resolve a link to
     * @param pageId        int representing the item id of the Page the link appears on
     * @param publicationId int representing the context Publication id to resolve the link in
     * @return String representing the URL of the link; or null, if the Component is not linked to
     * @throws ParseException if the TCMURIs are invalid
     */
    public String resolveComponentFromPageById(int componentId, int pageId, int publicationId) throws ParseException {
        TCMURI componentURI = new TCMURI(publicationId, componentId, 16, 0);
        TCMURI pageURI = new TCMURI(publicationId, pageId, 64, 0);

        return resolveComponentFromPage(componentURI.toString(), pageURI.toString());
    }

    /**
     * Builds a key using a named cache type (region) and an componentURI. This type of key is used to point to
     * the actual payload in the cache (e.g. cached Component Links).
     *
     * @param type         CacheType representing the type (or region) where the associated item is in cache
     * @param componentURI the Tridion TcmUri componentURI
     * @return String representing the key pointing to a cached value
     */
    private String getKey(CacheType type, String componentURI) {
        if (componentURI.endsWith("-16")) {
            return String.format("%s-%s", type, componentURI);
        } else {
            return String.format("%s-%s-16", type, componentURI);
        }
    }

    /**
     * Builds a key using a named cache type (region) and two ids. This type of key is used to point to
     * the actual payload in the cache (e.g. cached Component Links).
     *
     * @param type         CacheType representing the type (or region) where the associated item is in cache
     * @param componentURI the Tridion TcmUri componentURI
     * @param pageURI      the Tridion TcmUri pageURI
     * @return String representing the key pointing to a cached value
     */
    private String getKey(CacheType type, String componentURI, String pageURI) {
        if (componentURI.endsWith("-16")) {
            return String.format("%s-%s-%s", type, componentURI, pageURI);
        } else {
            return String.format("%s-%s-16-%s", type, componentURI, pageURI);
        }
    }
}
