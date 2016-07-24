package org.dd4t.providers.rs.service;

import org.dd4t.core.exceptions.ItemNotFoundException;
import org.dd4t.providers.rs.TridionComponentPresentationProvider;
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

/**
 * JAX-RS services class that defines and implements the service methods for fetching Dynamic Component Presentation
 * content from Content Delivery database.
 * <p/>
 * The DCP content is sent over as serialized string -- GZip compressed, and then Base64 encoded.
 * <p/>
 * The underlying @see TridionComponentProvider uses an EHCaching layer to improve performance of further read operations.
 * <p/>
 * @author Mihai Cadariu
 * @since 20.06.2014
 */
@Path ("/component")
public class ComponentProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(ComponentProviderService.class);

    /**
     * Service method that returns the content of a Dynamic Component Presentation identified by Publication id and
     * Component item id. Since no Component Template is provided, the DCP is looked up using the highest linking
     * priority.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.rs.ComponentProviderService#getComponentPresentationById
     * <p/>
     * <b>Note: This method performs significantly slower than getComponentPresentationById(int, int, int)! Do provide
     * a templateId!</b>
     *
     * @param componentId   int representing the Component item id
     * @param publicationId int representing the Publication item id
     * @return String representing the DCP content encoded as JSON, GZip and Base64; or null, otherwise
     */
    @GET
    @Path ("/getcomponentpresentationbyid/{publicationId:\\d+}/{componentId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getComponentPresentationById(@PathParam ("componentId") final int componentId,
                                               @PathParam ("publicationId") final int publicationId,
                                               @Context HttpServletRequest request) {
        return getComponentPresentationById(componentId, 0, publicationId, request);
    }

    /**
     * Service method that returns the content of a Dynamic Component Presentation identified by Publication id,
     * Component Template id and Component item id.
     * <p/>
     * The method delegates the call to @see org.dd4t.providers.TridionComponentProvider#getDynamicComponentPresentation
     *
     * @param componentId   int representing the Component item id
     * @param templateId    int representing the Component Template item id
     * @param publicationId int representing the Publication item id
     * @return String representing the DCP content encoded as JSON, GZip and Base64; or null, otherwise
     */
    @GET
    @Path ("/getcomponentpresentationbyid/{publicationId:\\d+}/{templateId:\\d+}/{componentId:\\d+}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getComponentPresentationById(@PathParam ("componentId") final int componentId,
                                               @PathParam ("templateId") final int templateId,
                                               @PathParam ("publicationId") final int publicationId,
                                               @Context HttpServletRequest request) {
        long time = System.currentTimeMillis();
        LOG.debug(">> Fetch Component Presentation by componentId: {}, templateId: {} and publicationId: {}",
                new Object[]{componentId, templateId, publicationId});

        RequestContextRegistry.setCurrentRequestContext(new BasicRequestContext(request));

        String result = null;
        try {
            result = TridionComponentPresentationProvider.INSTANCE.getDynamicComponentPresentation(componentId, templateId, publicationId);
        } catch (IOException e) {
            LOG.error("Error fetching Component Presentation", e);
        } catch (ItemNotFoundException e) {
            LOG.info("Component Presentation not found by componentId: {}, templateId: {} and publicationId: {}",
                    new Object[]{componentId, templateId, publicationId});
        }

        RequestContextRegistry.removeCurrentRequestContext();

        if (request.getParameter("pretty") != null) {
            LOG.debug("Pretty print result");
            JSONSerializer serializer = SerializerFactory.getSerializer();
            result = serializer.prettyPrint(result);
        }

        time = System.currentTimeMillis() - time;
        LOG.debug("<< End fetch Component Presentation content. Duration: {}s", time / 1000.0);

        return result;
    }
}
