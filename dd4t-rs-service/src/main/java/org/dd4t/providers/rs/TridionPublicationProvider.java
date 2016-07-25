package org.dd4t.providers.rs;

import org.dd4t.contentmodel.PublicationDescriptor;
import org.dd4t.providers.PublicationProvider;
import org.dd4t.providers.impl.BrokerPublicationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * dd4t-2-rs-service
 *
 * TODO: expose in service
 *
 * @author R. Kempees
 */
public class TridionPublicationProvider extends TridionBaseProvider implements PublicationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(TridionPublicationProvider.class);
    private static final TridionPublicationProvider INSTANCE = new TridionPublicationProvider();
    private PublicationProvider publicationProvider = new BrokerPublicationProvider();

    private TridionPublicationProvider() {

    }

    public static TridionPublicationProvider getInstance() {
        return INSTANCE;
    }

    @Override
    public int discoverPublicationIdByPageUrlPath (final String url) {

        return publicationProvider.discoverPublicationIdByPageUrlPath(url);
    }

    @Override
    public int discoverPublicationByBaseUrl (final String fullUrl) {

        return publicationProvider.discoverPublicationByBaseUrl(fullUrl);
    }

    @Override
    public int discoverPublicationByImagesUrl (final String fullUrl) {
        return publicationProvider.discoverPublicationByImagesUrl(fullUrl);
    }

    @Override
    public String discoverPublicationUrl (final int publicationId) {
        return publicationProvider.discoverPublicationUrl(publicationId);
    }

    @Override
    public String discoverPublicationPath (final int publicationId) {
        return publicationProvider.discoverPublicationPath(publicationId);
    }

    @Override
    public String discoverImagesUrl (final int publicationId) {
        return publicationProvider.discoverImagesUrl(publicationId);
    }

    @Override
    public String discoverImagesPath (final int publicationId) {
        return publicationProvider.discoverImagesPath(publicationId);
    }

    @Override
    public String discoverPublicationTitle (final int publicationId) {
        return publicationProvider.discoverPublicationTitle(publicationId);
    }

    @Override
    public String discoverPublicationKey (final int publicationId) {
        return publicationProvider.discoverPublicationKey(publicationId);
    }

    @Override
    public PublicationDescriptor getPublicationDescriptor (final int publicationId) {
        return publicationProvider.getPublicationDescriptor(publicationId);
    }
}
