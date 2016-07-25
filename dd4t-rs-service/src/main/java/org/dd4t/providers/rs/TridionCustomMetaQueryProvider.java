package org.dd4t.providers.rs;

import com.tridion.ItemTypes;
import com.tridion.broker.StorageException;
import com.tridion.broker.querying.Query;
import com.tridion.broker.querying.criteria.Criteria;
import com.tridion.broker.querying.criteria.content.PublicationCriteria;
import com.tridion.broker.querying.criteria.content.SchemaTitleCriteria;
import com.tridion.broker.querying.criteria.metadata.CustomMetaKeyCriteria;
import com.tridion.broker.querying.criteria.metadata.CustomMetaValueCriteria;
import com.tridion.broker.querying.criteria.operators.AndCriteria;
import com.tridion.broker.querying.criteria.operators.OrCriteria;
import com.tridion.broker.querying.criteria.taxonomy.TaxonomyKeywordCriteria;
import com.tridion.broker.querying.filter.LimitFilter;
import com.tridion.broker.querying.sorting.SortDirection;
import com.tridion.broker.querying.sorting.SortParameter;
import com.tridion.broker.querying.sorting.column.ComponentSchemaColumn;
import com.tridion.broker.querying.sorting.column.ItemLastPublishColumn;
import com.tridion.storage.CustomMetaValue;
import com.tridion.storage.StorageTypeMapping;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.dd4t.core.caching.CacheElement;
import org.dd4t.core.caching.CacheType;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.core.exceptions.SerializationException;
import org.dd4t.providers.rs.utils.DaoUtils;
import org.dd4t.providers.rs.utils.RsCacheType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TridionCustomMetaQueryProvider
 *
 * @author R. Kempees
 * @since 24/07/14.
 */
public class TridionCustomMetaQueryProvider extends TridionBaseProvider {

	private static final Logger LOG = LoggerFactory.getLogger(TridionCustomMetaQueryProvider.class);
	private static final int MAX_SEARCH_RESULTS = 11000;
	private static final LimitFilter LIMIT_FILTER = new LimitFilter(MAX_SEARCH_RESULTS);
	private static final String DIVIDER = "|";
	private static final String SELECT_ARTICLE_SKUS = "select distinct(cmv) from CustomMetaValue cmv where cmv.publicationId = :publicationId and cmv.itemType= :itemType and cmv.keyName= :keyName";

	private static final TridionCustomMetaQueryProvider INSTANCE = new TridionCustomMetaQueryProvider();

	private TridionCustomMetaQueryProvider () {

	}

	public static TridionCustomMetaQueryProvider getInstance() {
		return INSTANCE;
	}

	public String getComponentsByCustomMeta (String locale, final MultivaluedMap<String, String> queryStringCollection, int templateId) throws ItemNotFoundException, SerializationException, StorageException, ParseException, UnsupportedEncodingException {
		LOG.debug("Performing Custom Meta Query for {}, {}", locale, queryStringCollection.toString());

		final int publicationId = getPublicationId(locale);
		LOG.debug("Publication Id is: {}", publicationId);

		// Note:
		// Queries for Trondheim are always OR
		// Sorting is on Schema Field ASC (??)
		// MAX_RESULTS = 10000

		String key = getKey(CacheType.SEARCH_CUSTOM_META, publicationId, queryStringCollection);
		CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
		String result;

		if (cacheElement.isExpired()) {
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (cacheElement) {
				if (cacheElement.isExpired()) {
					cacheElement.setExpired(false);

					final StringBuilder components = new StringBuilder();
					final List<String> componentPresentations = getCustomMetaQueryComponentPresentations(queryStringCollection, templateId, publicationId);
					for (String componentPresentation : componentPresentations) {
						components.append(componentPresentation);
						components.append(DIVIDER);
					}

					LOG.debug("Returning: {}", components);
					result = components.toString();

					cacheElement.setPayload(result);
					cacheProvider.storeInItemCache(key, cacheElement);
				} else {
					LOG.debug("Retrieving from cache");
					result = cacheElement.getPayload();
				}
			}
		} else {
			LOG.debug("Retrieving from cache");
			result = cacheElement.getPayload();
		}

		if (result == null) {
			throw new ItemNotFoundException("Cannot find item (return previously cached value)");
		}

		return result;
	}

	public List<String> getCustomMetaQueryComponentPresentations (final MultivaluedMap<String, String> queryStringCollection, final int templateId, final int publicationId) throws ItemNotFoundException, ParseException, StorageException, UnsupportedEncodingException, SerializationException {

		final String itemUris[] = getCustomMetaQueryResults(publicationId, queryStringCollection);
		if (itemUris == null || itemUris.length == 0) {
			LOG.debug("No results found.");
		} else {
			LOG.debug("Found {} results.", itemUris.length);

			return TridionComponentPresentationProvider.getInstance().getDynamicComponentPresentations(itemUris, templateId, publicationId);

		}
		return new ArrayList<>();
	}

	public String getValuesForCustomMetaKey (String locale, String metaKey) throws ParseException, StorageException, ItemNotFoundException {
		if (StringUtils.isEmpty(metaKey)) {
			LOG.warn("No meta key given.");
			return null;
		}

		LOG.debug("Fetching all values for Custom Meta Key: {} and locale: {}", metaKey, locale);
		final int publicationId = getPublicationId(locale);
		LOG.debug("Publication Id is: {}", publicationId);

		final String key = getKey(RsCacheType.CUSTOM_META_VALUES_FOR_KEY, publicationId, metaKey, 0);
		CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
		String result = null;

		if (cacheElement.isExpired()) {
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (cacheElement) {
				if (cacheElement.isExpired()) {
					cacheElement.setExpired(false);
					try {
						final Map<String, Object> queryParams = new HashMap<>();
						queryParams.put("publicationId", publicationId);
						queryParams.put("itemType", ItemTypes.COMPONENT);
						queryParams.put("keyName", metaKey);

						final List<CustomMetaValue> queryResult = DaoUtils.getJPADAO(publicationId,StorageTypeMapping.ITEM_META).executeQueryListResult(SELECT_ARTICLE_SKUS, queryParams);

						final StringBuilder resultBuilder = new StringBuilder();

						for (CustomMetaValue customMetaValue : queryResult) {
							if (resultBuilder.length() > 0) {
								resultBuilder.append(",");
							}
							resultBuilder.append(customMetaValue.getStringValue());
						}

						result = resultBuilder.toString();
						LOG.debug(result);
						cacheElement.setPayload(result);
						cacheProvider.storeInItemCache(key, cacheElement);
					} catch (StorageException e) {
						LOG.error(e.getLocalizedMessage(), e);
					}
				} else {
					LOG.debug("Retrieving from cache");
					result = cacheElement.getPayload();
				}
			}
		} else {
			LOG.debug("Retrieving from cache");
			result = cacheElement.getPayload();
		}
		if (result == null) {
			throw new ItemNotFoundException("Cannot find item (return previously cached value)");
		}
		return result;
	}


	/**
	 * At the moment Trondheim uses two schemas: Product and Accessory
	 *
	 * @param locale
	 * @param schema
	 * @param templateId
	 * @return
	 * @throws ParseException
	 * @throws StorageException
	 * @throws ItemNotFoundException
	 */
	public String getComponentsBySchema (String locale, String schema, int templateId) throws ParseException, StorageException, ItemNotFoundException, IOException, SerializationException {
		LOG.debug("Performing Schema Query for {}, {}", locale, schema);
		final int publicationId = getPublicationId(locale);
		LOG.debug("Publication Id is: {}", publicationId);

		final String key = getKey(CacheType.COMPONENTS_BY_SCHEMA, publicationId, schema, templateId);
		CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
		String result;

		if (cacheElement.isExpired()) {
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (cacheElement) {
				if (cacheElement.isExpired()) {
					cacheElement.setExpired(false);


					final String[] itemUris = getComponentUrisBySchema(schema, publicationId);

					if (itemUris == null || itemUris.length == 0) {
						LOG.debug("No results found.");
						result = "";
						cacheElement.setExpired(true);
					} else {
						LOG.debug("Found {} results.", itemUris.length);
						final StringBuilder components = new StringBuilder();
						final List<String> componentPresentations = TridionComponentPresentationProvider.getInstance().getDynamicComponentPresentations(itemUris, templateId, publicationId);
						for (String componentPresentation : componentPresentations) {
							components.append(componentPresentation);
							components.append(DIVIDER);
						}

						LOG.trace("Returning: {}", components);
						result = components.toString();
					}


					LOG.debug("Storing result in cache.");
					cacheElement.setPayload(result);
					cacheProvider.storeInItemCache(key, cacheElement);
				} else {
					LOG.debug("Retrieving from cache");
					result = cacheElement.getPayload();
				}
			}
		} else {
			LOG.debug("Retrieving from cache");
			result = cacheElement.getPayload();
		}

		if (result == null) {
			throw new ItemNotFoundException("Cannot find item (return previously cached value)");
		}

		return result;
	}

	public static String[] getComponentUrisBySchema (final String schema, final int publicationId) throws StorageException {
		final PublicationCriteria publicationCriteria = new PublicationCriteria(publicationId);
		final SchemaTitleCriteria schemaTitleCriteria = new SchemaTitleCriteria(schema);

		final AndCriteria andCriteria = new AndCriteria(publicationCriteria, schemaTitleCriteria);
		final SortParameter sortParameter = new SortParameter(new ItemLastPublishColumn(), SortDirection.DESCENDING);
		final Query query = new Query(andCriteria);

		query.addSorting(sortParameter);
		query.setResultFilter(new LimitFilter(MAX_SEARCH_RESULTS));

		return query.executeQuery();
	}


	public String getComponentsBySchemaInKeyword (String locale, String schema, int categoryId, int keywordId, int templateId) throws ItemNotFoundException {
		LOG.debug("Performing Schema Query for {}, {}, in Keyword: {}", new Object[]{locale, schema, keywordId});
		final int publicationId = getPublicationId(locale);
		LOG.debug("Publication Id is: {}", publicationId);

		final String key = getKey(CacheType.COMPONENTS_BY_SCHEMA_IN_KEYWORD, publicationId, schema + "-" + keywordId, templateId);
		CacheElement<String> cacheElement = cacheProvider.loadPayloadFromLocalCache(key);
		String result = null;

		if (cacheElement.isExpired()) {
			//noinspection SynchronizationOnLocalVariableOrMethodParameter
			synchronized (cacheElement) {
				if (cacheElement.isExpired()) {
					cacheElement.setExpired(false);


					final Criteria[] criteria = new Criteria[]{new PublicationCriteria(publicationId), new SchemaTitleCriteria(schema), new TaxonomyKeywordCriteria(publicationId, categoryId, keywordId, false)};

					final AndCriteria andCriteria = new AndCriteria(criteria);
					final SortParameter sortParameter = new SortParameter(new ItemLastPublishColumn(), SortDirection.DESCENDING);
					final Query query = new Query(andCriteria);

					query.addSorting(sortParameter);
					query.setResultFilter(new LimitFilter(MAX_SEARCH_RESULTS));

					try {
						final String[] itemUris = query.executeQuery();

						if (itemUris == null || itemUris.length == 0) {
							LOG.debug("No results found.");
							cacheElement.setExpired(true);
						} else {
							LOG.debug("Found {} results.", itemUris.length);
							final StringBuilder components = new StringBuilder();

							List<String> componentPresentations = TridionComponentPresentationProvider.getInstance().getDynamicComponentPresentations(itemUris, templateId, publicationId);

							for (String componentPresentation : componentPresentations) {
								components.append(componentPresentation);
								components.append(DIVIDER);
							}

							LOG.trace("Returning: {}", components);
							result = components.toString();
						}

						LOG.debug("Storing result in cache.");
						cacheElement.setPayload(result);
						cacheProvider.storeInItemCache(key, cacheElement);
					} catch (StorageException | SerializationException | ItemNotFoundException e) {
						LOG.error(e.getLocalizedMessage(),e);
						cacheElement.setExpired(true);
					}

				} else {
					LOG.debug("Retrieving from cache");
					result = cacheElement.getPayload();
				}
			}
		} else {
			LOG.debug("Retrieving from cache");
			result = cacheElement.getPayload();
		}

		if (result == null) {
			throw new ItemNotFoundException("Cannot find item (return previously cached value)");
		}

		return result;
	}

    //FIXME
    private String getKey (RsCacheType type, int publicationId, String schema, int template) {
        return String.format("%s-%d-%s-%d", type, publicationId, schema, template);
    }

	private String getKey (CacheType type, int publicationId, String schema, int template) {
		return String.format("%s-%d-%s-%d", type, publicationId, schema, template);
	}

	private String getKey (CacheType type, int publicationId, MultivaluedMap<String, String> collection) {
		return String.format("%s-%d-%s", type, publicationId, getHash(collection));
	}

	private String getHash (MultivaluedMap<String, String> collection) {
		StringBuilder data = new StringBuilder();
		for (Map.Entry<String, List<String>> entry : collection.entrySet()) {
			data.append(entry.getKey());
			for (String value : entry.getValue()) {
				data.append(value);
			}
		}

		return DigestUtils.md5Hex(data.toString());
	}

	private String[] getCustomMetaQueryResults (final int publicationId, final MultivaluedMap<String, String> queryStringCollection) throws ItemNotFoundException, ParseException, StorageException {
		final PublicationCriteria publicationCriteria = new PublicationCriteria(publicationId);
		final SortParameter sortParameter = new SortParameter(new ComponentSchemaColumn(), SortDirection.ASCENDING);
		final ArrayList<CustomMetaValueCriteria> customMetaValueCriteria = new ArrayList<>();

		for (Map.Entry<String, List<String>> entry : queryStringCollection.entrySet()) {
			final String customMetaKey = entry.getKey();
			final CustomMetaKeyCriteria keyCriteria = new CustomMetaKeyCriteria(customMetaKey);
			for (final String value : entry.getValue()) {
				// TODO: isDate, isFloat
				LOG.debug("Adding key: {}, value: {}", customMetaKey, value);
				final CustomMetaValueCriteria valueCriteria = new CustomMetaValueCriteria(keyCriteria, value);
				customMetaValueCriteria.add(valueCriteria);
			}
		}

		final Query query = new Query();
		if (customMetaValueCriteria.size() > 1) {
			final OrCriteria orCriteria = new OrCriteria(customMetaValueCriteria.toArray(new CustomMetaValueCriteria[customMetaValueCriteria.size()]));
			query.setCriteria(new AndCriteria(publicationCriteria, orCriteria));
		} else if (customMetaValueCriteria.size() == 1) {
			query.setCriteria(new AndCriteria(publicationCriteria, customMetaValueCriteria.get(0)));
		} else {
			LOG.warn("No Custom Meta criteria added. Returning nothing.");
			return new String[]{};
		}

		query.addSorting(sortParameter);
		query.setResultFilter(LIMIT_FILTER);

		return query.executeQuery();
	}

	private int getPublicationId(String input) {
		int publicationId = getInteger(input);
		if (publicationId < 0) {
			publicationId = TridionPublicationProvider.getInstance().discoverPublicationIdByPageUrlPath(input);
		}
		return publicationId;
	}

	private int getInteger( String input ) {
		try {
			return Integer.parseInt(input);
		}
		catch( Exception e ) {
			return -1;
		}
	}
}
