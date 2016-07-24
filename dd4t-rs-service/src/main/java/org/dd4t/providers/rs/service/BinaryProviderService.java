package org.dd4t.providers.rs.service;

import com.tridion.broker.StorageException;
import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.providers.rs.TridionBinaryProvider;
import org.dd4t.providers.rs.request.BasicRequestContext;
import org.dd4t.providers.rs.request.RequestContextRegistry;
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
 * JAX-RS services class that defines and implements the service methods for fetching binary metadata and content from
 * Content Delivery Database.
 * <p/>
 * The binary content is sent over as is -- in form of Octet_Stream response mime type. The binary metadata is sent over
 * as a BASE64-encoded JSON object.
 * <p/>
 * The underlying @see TridionBinaryProvider uses an EHCaching layer to improve performance of further read operations
 *
 * @author Mihai Cadariu
 * @since 04.06.2014
 */
@Path ("/binary")
public class BinaryProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryProviderService.class);

    /**
     * Service method that returns the content byte array of a Tridion binary identified by Publication and item id.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionBinaryProvider#getBinaryContentById
     *
     * @param id          int representing the binary item id
     * @param publication int representing the binary Publication id
     * @return byte[] the array of bytes representing the binary content or null, if not found or error occurred
     */
    @GET
    @Path ("/getcontentbyid/{publicationId:\\d+}/{itemId:\\d+}")
    @Produces (MediaType.APPLICATION_OCTET_STREAM)
    public byte[] getContentById(@PathParam ("itemId") final int id,
                                 @PathParam ("publicationId") final int publication,
                                 @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary content with id: {} and publication: {}", id, publication);
        byte[] result = null;

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        try {
            result = TridionBinaryProvider.INSTANCE.getBinaryContentById(id, publication);
        } catch (StorageException e) {
            LOG.error("Error fetching binary content", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary content not found by id: {} and publication: {}", id, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch binary content. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns the content byte array of a Tridion binary identified by Publication id and URL.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionBinaryProvider#getBinaryContentByURL
     *
     * @param publication int representing the Publication id to lookup the binary in
     * @param url         string representing the path part of the binary URL
     * @return byte[] the array of bytes representing the binary content or null, if not found or error occurred
     */
    @GET
    @Path ("/getcontentbyurl/{publicationId:\\d+}/{url}")
    @Produces (MediaType.APPLICATION_OCTET_STREAM)
    public byte[] getContentByURL(@PathParam ("url") final String url,
                                  @PathParam ("publicationId") final int publication,
                                  @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary content with url: {} and publication: {}", url, publication);
        byte[] result = null;

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        try {
            result = TridionBinaryProvider.INSTANCE.getBinaryContentByURL(url, publication);
        } catch (StorageException e) {
            LOG.error("Error fetching binary content", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary content not found by url: {} and publication: {}", url, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch binary content. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a JSON representation of the Tridion binary identified by Publication and item id.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionBinaryProvider#getBinaryVariantById and then
     * serializes the @see BinaryVariant to JSON.
     *
     * @param id          int representing the binary item id
     * @param publication int representing the binary Publication id
     * @return String representing the JSON serialization of the indentified binary or null, if not found or error occurred
     */
    @GET
    @Path ("/getmetabyid/{publicationId:\\d+}/{itemId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getBinaryMetaById(@PathParam ("itemId") final int id,
                                    @PathParam ("publicationId") final int publication,
                                    @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary meta by id: {} and publication: {}", id, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionBinaryProvider.INSTANCE.getBinaryMetaById(id, publication);
        } catch (IOException | StorageException e) {
            LOG.error("Error fetching binary meta", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary meta not found by id: {} and publication: {}", id, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetching binary meta. Duration {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a JSON representation of the Tridion binary identified by Publication id and URL.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionBinaryProvider#getBinaryMetaByURL and then
     * serializes the @see BinaryVariant to JSON.
     *
     * @param url         string representing the path part of the binary URL
     * @param publication int representing the Publication id to lookup the binary in
     * @return String representing the JSON serialization of the indentified binary or null, if not found or error occurred
     */
    @GET
    @Path ("/getmetabyurl/{publicationId:\\d+}/{url}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getMetaByURL(@PathParam ("url") final String url,
                               @PathParam ("publicationId") final int publication,
                               @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary meta by url: {} and publication: {}", url, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionBinaryProvider.INSTANCE.getBinaryMetaByURL(url, publication);
        } catch (IOException | StorageException e) {
            LOG.error("Error fetching binary meta", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary meta not found by url: {} and publication: {}", url, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch binary meta. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Returns a GZipped BinaryWrapper object. The wrapper contains (1) MultimediaImpl that is first GZipped, then Base64
     * encoded, and (2) a byte array of the binary raw content.
     *
     * @param id          int representing the binary item id
     * @param publication int representing the binary Publication id
     * @return byte[] the byte array of a GZipped BinaryWrapepr object
     */
    @GET
    @Path ("/getwrapperbyid/{publicationId:\\d+}/{itemId:\\d+}")
    @Produces (MediaType.APPLICATION_OCTET_STREAM)
    public byte[] getWrapperById(@PathParam ("itemId") final int id,
                                 @PathParam ("publicationId") final int publication,
                                 @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary wrapper by id: {} and publication: {}", id, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        byte[] result = null;
        try {
            result = TridionBinaryProvider.INSTANCE.getWrapperById(id, publication);
        } catch (IOException | StorageException e) {
            LOG.error("Error fetching binary", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary not found by id: {} and publication: {}", id, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetching binary wrapper. Duration {}s", time / 1000.0);

        return result;
    }

    /**
     * Returns a GZipped BinaryWrapper object. The wrapper contains (1) MultimediaImpl that is first GZipped, then Base64
     * encoded, and (2) a byte array of the binary raw content.
     *
     * @param url         string representing the path part of the binary URL
     * @param publication int representing the binary Publication id
     * @return byte[] the byte array of a GZipped BinaryWrapepr object
     */
    @GET
    @Path ("/getwrapperbyurl/{publicationId:\\d+}/{url}")
    @Produces (MediaType.APPLICATION_OCTET_STREAM)
    public byte[] getWrapperByURL(@PathParam ("url") final String url,
                                  @PathParam ("publicationId") final int publication,
                                  @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch binary wrapper by url: {} and publication: {}", url, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        byte[] result = null;
        try {
            result = TridionBinaryProvider.INSTANCE.getWrapperByURL(url, publication);
        } catch (IOException | StorageException e) {
            LOG.error("Error fetching binary", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Binary not found by url: {} and publication: {}", url, publication);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetching binary wrapper. Duration {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns the Publication TCMURI item id for the given Images URL stub.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionBinaryProvider#discoverPublicationId
     *
     * @param imagesUrl String representing the Images Url metadata as defined in Tridion
     * @return int representing the Publication id if found; or 0, otherwise
     */
    @GET
    @Path ("/discoverpublicationid/{imagesUrl}")
    @Produces (MediaType.TEXT_PLAIN)
    public int discoverPublicationId(@PathParam ("imagesUrl") String imagesUrl) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Discover Publication id for ImagesUrl: {}", imagesUrl);

        int result = 0;
        try {
            result = TridionBinaryProvider.INSTANCE.discoverPublicationId(imagesUrl);
        } catch (ParseException | StorageException e) {
            LOG.error("Error discovering Publication", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Publication not found for ImagesUrl: {}", imagesUrl);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End discover Publication id. Duration: {}s", time / 1000.0);

        return result;
    }
}
