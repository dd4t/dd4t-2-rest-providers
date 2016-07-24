package org.dd4t.providers.rs;

import org.dd4t.contentmodel.exceptions.ItemNotFoundException;
import org.dd4t.providers.TridionTaxonomyProvider;
import org.dd4t.providers.request.BasicRequestContext;
import org.dd4t.providers.request.RequestContextRegistry;
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
 * JAX-RS services class that defines and implements the service methods for fetching taxonomy keywords and related
 * content items from the Content Delivery database. The keywords have all their Parent/Children relationships resolved.
 * <p/>
 * The taxonomy is sent over as serialized string -- first the Keyword object is GZip compressed, then Base64 encoded.
 * <p/>
 * The underlying @see TridionTaxonomyProvider uses an EHCaching layer to improve performance of further read operations.
 */
@Path ("/taxonomy")
public class TaxonomyProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(TaxonomyProviderService.class);

    /**
     * Service method that returns the content of a Tridion Taxonomy identified by TCMURI. It also resolves the TCMURIs
     * of the items classified against each keyword in the taxonomy.
     * <p/>
     * The method delegates the call to @see TaxonomyProviderService#getTaxonomyByURI with param resolveContent true
     *
     * @param taxonomyURI String representing the root taxonomy Keyword TCMURI
     * @return String representing the taxonomy compressed with GZip and Base64 encoded; or null, otherwise
     */
    @GET
    @Path ("/gettaxonomy/{taxonomyURI}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getTaxonomyByURI(@PathParam ("taxonomyURI") final String taxonomyURI,
                                   @Context HttpServletRequest request) {
        return getTaxonomyByURI(taxonomyURI, true, request);
    }

    /**
     * Service method that returns the content of a Tridion Taxonomy identified by TCMURI.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getPageContentById
     *
     * @param taxonomyURI    String representing the root taxonomy Keyword TCMURI
     * @param resolveContent boolean indicating whether or not to include classified content TCMURIs for each Keyword
     * @return String representing the taxonomy compressed with GZip and Base64 encoded; or null, otherwise
     */
    @GET
    @Path ("/gettaxonomy/{taxonomyURI}/{resolveContent}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getTaxonomyByURI(@PathParam ("taxonomyURI") final String taxonomyURI,
                                   @PathParam ("resolveContent") final boolean resolveContent,
                                   @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch taxonomy by URI: {} and resolveContent: {}", taxonomyURI, resolveContent);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionTaxonomyProvider.INSTANCE.getTaxonomy(taxonomyURI, resolveContent);
        } catch (IOException | ItemNotFoundException | ParseException e) {
            LOG.error("Error fetching taxonomy", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch taxonomy. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns the content of a Tridion Taxonomy identified by TCMURI. The returned Keywords
     * contain as related items only the Components based on the given SchemaURI.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionPageProvider#getTaxonomyRelatedBySchema
     *
     * @param taxonomyURI String representing the root taxonomy Keyword TCMURI
     * @param schemaURI   String representing the filter for classified related Components to return for each Keyword
     * @return String representing the taxonomy compressed with GZip and Base64 encoded; or null, otherwise
     */
    @GET
    @Path ("/gettaxonomybyschema/{taxonomyURI}/{schemaURI}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getTaxonomyFilterBySchema(@PathParam ("taxonomyURI") final String taxonomyURI,
                                            @PathParam ("schemaURI") final String schemaURI,
                                            @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch taxonomy by URI: {} and filter related Components by schemaURI: {}", taxonomyURI, schemaURI);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionTaxonomyProvider.INSTANCE.getTaxonomyRelatedBySchema(taxonomyURI, schemaURI);
        } catch (IOException | ItemNotFoundException | ParseException e) {
            LOG.error("Error fetching taxonomy", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch taxonomy. Duration: {}s", time / 1000.0);

        return result;
    }
}
