package org.dd4t.providers.rs;

import com.tridion.ItemTypes;
import com.tridion.broker.StorageException;
import com.tridion.storage.RelatedKeyword;
import org.dd4t.contentmodel.Keyword;
import org.dd4t.contentmodel.impl.KeywordImpl;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.core.caching.CacheType;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.core.exceptions.SerializationException;
import org.dd4t.core.util.TCMURI;
import org.dd4t.providers.impl.BrokerTaxonomyProvider;
import org.dd4t.providers.serializer.KeywordBuilder;
import org.dd4t.providers.serializer.SerializerFactory;
import org.dd4t.providers.serializer.json.JSONSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tridion provider for taxonomies stored in the Content Delivery Database. The class retrieves the Keyword object model
 * with all its Parent/Children relationships resloved. Additionally it can also resolve or not the content used by
 * each Keyword (i.e. the Page of Component TCMURIs that make direct 'use' a particular Keyword) in the taxonomy.
 * <p/>
 * The returned items are also placed in an EHCache for future reads increased performance.
 *
 * @author Mihai Cadariu
 * @since 16.06.14
 */
public class TridionTaxonomyProvider extends TridionBaseProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TridionTaxonomyProvider.class);
    private static final TridionTaxonomyProvider INSTANCE = new TridionTaxonomyProvider();

    private final BrokerTaxonomyProvider taxonomyProvider = new BrokerTaxonomyProvider();

    private TridionTaxonomyProvider() {

    }

    public static TridionTaxonomyProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerTaxonomyProvider#getTaxonomy
     * <p/>
     * The returned serialized Keyword object is placed in EHCache for faster future retrieval
     *
     * @param taxonomyURI    String representing the root taxonomy Keyword TCMURI
     * @param resolveContent boolean indicating whether or not to include classified content TCMURIs for each Keyword
     * @return String the serialized keyword with its parent/children relationships resolved
     * @throws IOException           if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by taxonomyURI was not found
     * @throws ParseException        if the TCMURI is invalid
     */
    public String getTaxonomy(final String taxonomyURI, final boolean resolveContent)
            throws IOException, ItemNotFoundException, ParseException {
        LOG.debug("Fetching taxonomy by uri: {}", taxonomyURI);

        String key = getKey(CacheType.TAXONOMY, taxonomyURI);
        CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        String result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        com.tridion.taxonomies.Keyword tridionKeyword = taxonomyProvider.getTaxonomy(taxonomyURI);
                        if (tridionKeyword == null) {
                            cacheElement.setPayload(null);
                            cacheProvider.storeInItemCache(key, cacheElement);
                            throw new ItemNotFoundException("Taxonomy with URI " + taxonomyURI + " was not found");
                        }

                        KeywordBuilder builder;
                        if (resolveContent) {
                            Map<String, Set<TCMURI>> relatedItems = getRelatedContent(taxonomyURI);
                            builder = new KeywordBuilder(relatedItems);
                        } else {
                            builder = new KeywordBuilder();
                        }

                        Keyword keyword = builder.build(tridionKeyword);
                        result = serialize(keyword);

                        TCMURI tcmUri = new TCMURI(taxonomyURI);
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                    } catch (SerializationException | StorageException se) {
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);

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
            throw new ItemNotFoundException("Taxonomy with URI " + taxonomyURI + " was not found");
        }

        return result;
    }

    /**
     * Method delegates to @see org.dd4t.providers.impl.BrokerTaxonomyProvider#getTaxonomy
     * <p/>
     * The returned serialized Keyword object is placed in EHCache for faster future retrieval
     *
     * @param taxonomyURI String representing the root taxonomy Keyword TCMURI
     * @param schemaURI   String representing the filter for classified related Components to returnfor each Keyword
     * @return String the serialized keyword with its parent/children relationships resolved
     * @throws IOException           if something went wrong during accessing the CD DB
     * @throws ItemNotFoundException if the item identified by taxonomyURI was not found
     * @throws ParseException        if the TCMURIs are invalid
     */
    public String getTaxonomyRelatedBySchema(final String taxonomyURI, final String schemaURI)
            throws IOException, ItemNotFoundException, ParseException {
        LOG.debug("Fetching taxonomy by: {} and filter related Components by schemaURI: {}", taxonomyURI, schemaURI);

        String key = getKey(CacheType.TAXONOMY, taxonomyURI, schemaURI);
        CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        String result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        com.tridion.taxonomies.Keyword tridionKeyword = taxonomyProvider.getTaxonomy(taxonomyURI);
                        if (tridionKeyword == null) {
                            cacheElement.setPayload(null);
                            cacheProvider.storeInItemCache(key, cacheElement);
                            throw new ItemNotFoundException("Taxonomy with URI " + taxonomyURI + " was not found");
                        }

                        Map<String, Set<TCMURI>> relatedItems = getRelatedContentBySchema(taxonomyURI, schemaURI);
                        KeywordBuilder builder = new KeywordBuilder(relatedItems);
                        Keyword keyword = builder.build(tridionKeyword);
                        result = serialize(keyword);

                        TCMURI tcmUri = new TCMURI(taxonomyURI);
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                    } catch (IOException | SerializationException | StorageException se) {
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);

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
            throw new ItemNotFoundException("Taxonomy with URI " + taxonomyURI + " was not found");
        }

        return result;
    }

    /**
     * Retrieves a map having Keyword TCMURIs as keys and as values a set of TCMURIs representing the items that make
     * direct 'use' the the Keyword in the key.
     *
     * @param taxonomyURI String representing the root taxonomy Keyword TCMURI
     * @return Map of items that use each keyword directly
     * @throws IOException if something went wrong during accessing the CD DB
     */
    private Map<String, Set<TCMURI>> getRelatedContent(final String taxonomyURI) throws IOException {
        LOG.debug("Fetching related items for keywords in taxonomy with taxonomyURI: {}", taxonomyURI);

        String key = getKey(CacheType.RELATED_KEYWORD, taxonomyURI);
        CacheElement<Map<String, Set<TCMURI>>> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        Map<String, Set<TCMURI>> result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        result = new HashMap<>();
                        List<RelatedKeyword> relatedComponents = taxonomyProvider.getRelatedItems(taxonomyURI, ItemTypes.COMPONENT);
                        mergeRelatedItems(relatedComponents, result, ItemTypes.COMPONENT);
                        List<RelatedKeyword> relatedPages = taxonomyProvider.getRelatedItems(taxonomyURI, ItemTypes.PAGE);
                        mergeRelatedItems(relatedPages, result, ItemTypes.PAGE);

                        TCMURI tcmUri = new TCMURI(taxonomyURI);
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                    } catch (ParseException | StorageException e) {
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);

                        throw new IOException(e);
                    }
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
     * Retrieves a map having Keyword TCMURIs as keys and as values a set of TCMURIs representing the items that make
     * direct 'use' the the Keyword in the key. The related items are only Components based on the given SchemaURI.
     *
     * @param taxonomyURI String representing the root taxonomy Keyword TCMURI
     * @param schemaURI   String representing the filter for classified related Components to return for each Keyword
     * @return Map of items that use each keyword directly
     * @throws IOException if something went wrong during accessing the CD DB
     */
    private Map<String, Set<TCMURI>> getRelatedContentBySchema(final String taxonomyURI, final String schemaURI)
            throws IOException {
        LOG.debug("Fetching related component by schemaURI: {} for keywords in taxonomy with taxonomyURI: {}", taxonomyURI, schemaURI);

        String key = getKey(CacheType.RELATED_KEYWORD_BY_SCHEMA, taxonomyURI, schemaURI);
        CacheElement<Map<String, Set<TCMURI>>> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
        Map<String, Set<TCMURI>> result;

        if (cacheElement.isExpired()) {
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cacheElement) {
                if (cacheElement.isExpired()) {
                    cacheElement.setExpired(false);
                    try {
                        result = new HashMap<>();
                        List<RelatedKeyword> relatedComponents = taxonomyProvider.getRelatedComponentsBySchema(taxonomyURI, schemaURI);
                        mergeRelatedItems(relatedComponents, result, ItemTypes.COMPONENT);

                        TCMURI tcmUri = new TCMURI(taxonomyURI);
                        cacheElement.setPayload(result);
                        cacheProvider.storeInItemCache(key, cacheElement, tcmUri.getPublicationId(), tcmUri.getItemId());
                    } catch (ParseException | StorageException e) {
                        cacheElement.setPayload(null);
                        cacheProvider.storeInItemCache(key, cacheElement);

                        throw new IOException(e);
                    }
                } else {
                    result = cacheElement.getPayload();
                }
            }
        } else {
            result = cacheElement.getPayload();
        }

        return result;
    }

    /*
    Merges the list of RelatedKeyword objects into a map with key=Keyword TCMURI and value=Set of Tridion items that
    use that Keyword directly
     */
    private void mergeRelatedItems(List<RelatedKeyword> relatedKeywords, Map<String, Set<TCMURI>> result, int itemType) {
        for (RelatedKeyword keyword : relatedKeywords) {
            int publicationId = keyword.getPublicationId();
            String key = String.format("tcm:%d-%d-1024", publicationId, keyword.getKeywordId());
            Set<TCMURI> itemList = result.get(key);
            if (itemList == null) {
                itemList = new TreeSet<>();
                result.put(key, itemList);
            }
            TCMURI itemURI = new TCMURI(publicationId, keyword.getItemId(), itemType, 0);
            itemList.add(itemURI);
        }
    }

    /*
    Serializes a Keyword object by performing the following steps: serialize to JSON, compress with GZip, encode with
    Base64. On the client side, perform the same steps for deserializing, only in reversed order.
     */
    private String serialize(Keyword keyword) throws SerializationException {
        JSONSerializer serializer = SerializerFactory.getSerializer();
        String json = serializer.serializeJSON(keyword);
        byte[] gzip = serializer.compressGZip(json);
        return serializer.encodeBase64(gzip);
    }

    /*
    Deserializes a string representing a Keyword object by performing the following steps: decode with Base64,
    decompress with GZip, deserialize JSON to Keyword object.
     */
    private Keyword deserialize(String keyword) throws SerializationException {
        JSONSerializer serializer = SerializerFactory.getSerializer();
        byte[] bytes = serializer.decodeBase64(keyword);
        String json = serializer.decompressGZip(bytes);
        return serializer.deserializeJSON(json, KeywordImpl.class);
    }

    /**
     * Builds a key using a named cache type (region) and an taxonomyURI. This type of key is used to point to
     * the actual payload in the cache (e.g. cached Component Links).
     *
     * @param type        CacheType representing the type (or region) where the associated item is in cache
     * @param taxonomyURI the Tridion TcmUri taxonomyURI
     * @return String representing the key pointing to a cached value
     */
    private String getKey(CacheType type, String taxonomyURI) {
        return String.format("%s-%s", type, taxonomyURI);
    }

    /**
     * Builds a key using a named cache type (region) and two ids. This type of key is used to point to
     * the actual payload in the cache (e.g. cached Component Links).
     *
     * @param type        CacheType representing the type (or region) where the associated item is in cache
     * @param taxonomyURI the Tridion TcmUri taxonomyURI
     * @param schemaURI   the Tridion TcmUri schemaURI
     * @return String representing the key pointing to a cached value
     */
    private String getKey(CacheType type, String taxonomyURI, String schemaURI) {
        return String.format("%s-%s-%s", type, taxonomyURI, schemaURI);
    }
}
