package org.dd4t.providers;

import com.tridion.broker.StorageException;
import com.tridion.storage.PageMeta;
import org.apache.commons.codec.binary.Base64;
import org.dd4t.contentmodel.exceptions.ItemNotFoundException;
import org.dd4t.providers.impl.BrokerPageProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by rai on 01/05/14.
 */

// We don't use @singleton
/*
 * TODO: Test properly whether publication Ids are always correct
 * TODO: url must be properly encoded / decoded. Preferably this becomes
 * a unique garbled string
 *
 * We don't directly hook into the BrokerPageProvider class
 * because we may need to do decompression, XML > Json,
 * and other assorted operations.
 *
 * Plus, we want to have a singleton instance
 * (but that could also move into the service class).
 *
 */
public enum TridionPageProvider implements PageProvider
{
	INSTANCE;
	private final BrokerPageProvider pageProvider = new BrokerPageProvider();
	private final Base64 urlCoder = new Base64(true);
	private final static Logger LOG = LoggerFactory.getLogger(TridionPageProvider.class);

	private TridionPageProvider() { }

	@Override
	public PageMeta getPageMetaByURL (final String url, final int publication) throws StorageException, ItemNotFoundException
	{
		// TODO: flatten page meta XML inside Page XML
		LOG.debug("Fetching Page Meta by Url: {} and publication: {}", url, publication);
		return pageProvider.getPageMetaByURL(decodeUrl(url),publication);
	}

	@Override
	public PageMeta getPageMetaById (final int id, final int publication) throws StorageException, ItemNotFoundException
	{
		LOG.debug("Fetching Page Meta by Id: {}, for publication: {}", id, publication);
		return pageProvider.getPageMetaById(id,publication);
	}

	@Override
	public String getPageXMLByURL (final String url, final int publication) throws StorageException, IOException, ItemNotFoundException
	{
		LOG.debug("Fetching url:{}, pub id: {}", url, publication);
		return pageProvider.getPageXMLByURL(decodeUrl(url),publication);
	}

	@Override
	public String getPageXMLByMeta (final PageMeta meta) throws StorageException, IOException
	{
		if (null != meta)
		{
			LOG.debug("Fetching Page XML by Meta id: {}", meta.getItemId());
			return pageProvider.getPageXMLByMeta(meta);
		}
		LOG.error("Page meta was null.");
		return null;
	}

	private String decodeUrl(final String url) throws ItemNotFoundException
	{
		if (null != url)
		{
			String decodedUrl = new String(urlCoder.decode(url));
			if (null != decodedUrl)
			{
				LOG.debug("Decoded Url: {} ", decodedUrl);
				return decodedUrl;
			}
		}
		throw new ItemNotFoundException("Url parameter could not be decoded. Item not found or parameter was null.");
	}
}
