package org.dd4t.providers.rs.service;

import org.dd4t.providers.rs.TridionLinkProvider;
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
import java.text.ParseException;

/**
 * JAX-RS services class that defines and implements the service methods for reslving Component links either stand-alone
 * or on a particular Page.
 * <p/>
 * The underlying @see TridionLinkProvider uses an EHCaching layer to improve performance of further read operations.
 */
@Path ("/link")
public class LinkProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(LinkProviderService.class);

    /**
     * Service method that returns a resolved link URL of a stand-alone Component link. If the link cannot be resolved,
     * it returns null.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionLinkProvider#resolveComponent
     *
     * @param targetComponentURI String representing the TcmUri of the Component to resolve a link to
     * @return String representing the URL of the link; or null, if the Component is not linked to
     */
    @GET
    @Path ("/resolvecomponentbyuri/{targetComponentURI}")
    @Produces (MediaType.TEXT_PLAIN)
    public String resolveComponentByURI(@PathParam ("targetComponentURI") final String targetComponentURI,
                                        @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch Component link to {}", targetComponentURI);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionLinkProvider.INSTANCE.resolveComponent(targetComponentURI);
        } catch (ParseException e) {
            LOG.error("Error resolving link to Component", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch Component link. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a resolved link URL of a stand-alone Component link. If the link cannot be resolved,
     * it returns null.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionLinkProvider#resolveComponentById
     *
     * @param publication int representing the context Publication id to resolve the link in
     * @param id          int representing the Component item id to to resolve a link to
     * @return String representing the URL of the link; or null, if the Component is not linked to
     */
    @GET
    @Path ("/resolvecomponentbyid/{publicationId:\\d+}/{componentId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String resolveComponentById(@PathParam ("publicationId") final int publication,
                                       @PathParam ("componentId") final int id,
                                       @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch Component link to {} in Publication {}", id, publication);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionLinkProvider.INSTANCE.resolveComponentById(id, publication);
        } catch (ParseException e) {
            LOG.error("Error resolving link to Component", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch Component link. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a resolved link URL of a Component link on a Page. If the link cannot be resolved,
     * it returns null.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionLinkProvider#resolveComponentFromPage
     *
     * @param targetComponentURI String representing the TcmUri of the Component to resolve a link to
     * @param sourcePageURI      String representing the TcmUri of the source Page (the current page)
     * @return String representing the URL of the link; or null, if the Component is not linked to
     */
    @GET
    @Path ("/resolvecomponentfrompagebyuri/{targetComponentURI}/{sourcePageURI}")
    @Produces (MediaType.TEXT_PLAIN)
    public String resolveComponentFromPageByURI(@PathParam ("targetComponentURI") final String targetComponentURI,
                                                @PathParam ("sourcePageURI") final String sourcePageURI,
                                                @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch Component link to {} from Page {}", targetComponentURI, sourcePageURI);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionLinkProvider.INSTANCE.resolveComponentFromPage(targetComponentURI, sourcePageURI);
        } catch (ParseException e) {
            LOG.error("Error resolving link to Component", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch Component link from Page. Duration: {}s", time / 1000.0);

        return result;
    }

    /**
     * Service method that returns a resolved link URL of a Component link on a Page. If the link cannot be resolved,
     * it returns null.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionLinkProvider#resolveComponentFromPage
     *
     * @param publicationId int representing the context Publication id to resolve the link in
     * @param componentId   int representing the Component item id to resolve a link to
     * @param pageId        int representing the item id of the Page where the link appears on
     * @return String representing the URL of the link; or null, if the Component is not linked to
     */
    @GET
    @Path ("/resolvecomponentfrompagebyid/{publicationId:\\d+}/{componentId:\\d+}/{pageId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String resolveComponentFromPageById(@PathParam ("publicationId") final int publicationId,
                                               @PathParam ("componentId") final int componentId,
                                               @PathParam ("pageId") final int pageId,
                                               @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch Component link to {} from Page {}", componentId, pageId);

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionLinkProvider.INSTANCE.resolveComponentFromPageById(componentId, pageId, publicationId);
        } catch (ParseException e) {
            LOG.error("Error resolving link to Component", e);
        }

        RequestContextRegistry.removeCurrentRequestContext();

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch Component link from Page. Duration: {}s", time / 1000.0);

        return result;
    }
}
