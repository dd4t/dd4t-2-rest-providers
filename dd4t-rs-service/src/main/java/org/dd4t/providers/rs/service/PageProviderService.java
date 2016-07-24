package org.dd4t.providers.rs.service;

import com.tridion.broker.StorageException;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.core.exceptions.SerializationException;
import org.dd4t.providers.rs.TridionPageProvider;
import org.dd4t.providers.rs.request.BasicRequestContext;
import org.dd4t.providers.rs.request.RequestContextRegistry;
import org.dd4t.providers.serializer.SerializerFactory;
import org.dd4t.providers.serializer.json.JSONSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.text.ParseException;

/**
 * JAX-RS services class that defines and implements the service methods for fetching page metadata and content from
 * Content Delivery database.
 * <p/>
 * The page content is sent over as serialized string -- first converted to JSON format, then GZip compressed, and
 * finally Base64 encoded.
 * <p/>
 * The underlying @see TridionPageProvider uses an EHCaching layer to improve performance of further read operations.

 * @author R. Kempees
 * @since 01.05.2014
 */
@Path ("/page")
public class PageProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(PageProviderService.class);

    /**
     * Service method that returns the content of a Tridion Page identified by Publication id and item id.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getPageContentById
     *
     * @param id          int representing the page item id
     * @param publication int representing the page Publication id
     * @param request     HttpServletRequest representing the current request
     * @return String representing the page content encoded as JSON, GZip and Base64; or null, otherwise
     */
    @GET
    @Path ("/getcontentbyid/{publicationId:\\d+}/{itemId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getPageContentById(@PathParam ("itemId") final int id,
                                     @PathParam ("publicationId") final int publication,
                                     @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch page content by id: {} and publication: {}", id, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionPageProvider.getInstance().getPageContentById(id, publication);
        } catch (SerializationException | IOException e) {
            LOG.error("Error fetching page", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Page not found by id: {} and publication: ", id, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch page content. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns the content of a Tridion Page identified by Publication id and URL.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getPageContentByURL
     *
     * @param url         String representing the path part of the page URL
     * @param publication int representing the page Publication id
     * @param request     HttpServletRequest representing the current request
     * @return String representing the page content encoded as JSON, GZip and Base64; or null, otherwise
     */
    @GET
    @Path ("/getcontentbyurl/{publicationId:\\d+}/{url}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getPageContentByURL(@PathParam ("url") String url,
                                      @PathParam ("publicationId") int publication,
                                      @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch page content by url: {} and publication: {}", url, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionPageProvider.getInstance().getPageContentByURL(url, publication);
        } catch (SerializationException e) {
            LOG.error("Error fetching page", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Page not found by url: {} and publication: ", url, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch page content. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a list of URLs for all published Tridion Pages in a Publication.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getPageListByPublicationId
     *
     * @param publication int representing the Publication item id
     * @param request     HttpServletRequest representing the current request
     * @return String representing the list of page URLs encoded as GZip and Base64; or null, otherwise
     */
    @GET
    @Path ("/getlistbypublication/{publicationId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getPageListByPublicationId(@PathParam ("publicationId") int publication,
                                             @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch page URL list by publication: {}", publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionPageProvider.getInstance().getPageListByPublicationId(publication);
        } catch (SerializationException e) {
            LOG.error("Error fetching page URL list", e);
        } catch (ItemNotFoundException  e) {
            LOG.info("Page URL list not found by publication: ", publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch page URL list. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a boolean value to check whether a page exists.
     * This is identified by Publication id and URL.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getPageContentByURL
     * <p/>
     * TODO: maybe this should be cached.
     *
     * @param url         String representing the path part of the page URL
     * @param publication int representing the page Publication id
     * @return Boolean true if page exists. False otherwise.
     */
    @GET
    @Path ("/checkpageexists/{publicationId:\\d+}/{url}")
    @Produces (MediaType.TEXT_PLAIN)
    public Integer checkPageExists(@PathParam ("url") String url,
                                   @PathParam ("publicationId") int publication) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Check page exists by url: {} and publication: {}", url, publication);

        Integer result = 0;
        try {

            final boolean pageExists = TridionPageProvider.getInstance().checkPageExists(url, publication);
            if (pageExists) {
                return 1;
            }
        } catch (SerializationException e) {
            LOG.error("Error fetching page", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Page not found by url: {} and publication: ", url, publication);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End check page exists. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns the Publication TCMURI item id for the given Publication URL stub.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#discoverPublicationId
     *
     * @param publicationUrl String representing the Publication Url metadata as defined in Tridion
     * @return int representing the Publication id if found; or 0, otherwise
     */
    @GET
    @Path ("/discoverpublicationid/{publicationUrl}")
    @Produces (MediaType.TEXT_PLAIN)
    public int discoverPublicationId(@PathParam ("publicationUrl") String publicationUrl) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Discover Publication id for publicationUrl: {}", publicationUrl);

        int result = 0;
        try {
            result = TridionPageProvider.INSTANCE.discoverPublicationId(publicationUrl);
        } catch (ParseException | StorageException e) {
            LOG.error("Error discovering Publication", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Publication not found for publicationUrl: {}", publicationUrl);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End discover Publication id. Duration: {}s", time / 1000.0);

        return result;
    }
}
