package org.dd4t.providers.rs.service;

import com.tridion.broker.StorageException;
import org.apache.commons.codec.binary.Base64;
import org.dd4t.contentmodel.exceptions.ItemNotFoundException;
import org.dd4t.providers.rs.TridionCustomMetaQueryProvider;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.text.ParseException;

/**
 * CustomMetaQueryProviderService
 * <p/>
 * JAX-RS entry point for Broker queries based on Custom Metadata
 *
 * @author R. Kempees
 * @since 23/07/14.
 */
@Path ("/query")
public class CustomMetaQueryProviderService {

    private static final Logger LOG = LoggerFactory.getLogger(CustomMetaQueryProviderService.class);

    private final Base64 urlCoder = new Base64(true);

    /**
     * Entry point to perform a Custom Meta query on the Tridion to fetch
     * Dynamic Tridion Components (NOT Component Presentations!).
     * <p/>
     * Custom meta key value pairs are in the querystring. Multiple values
     * for one Custom Meta Key are possible.
     * <p/>
     * The result must be split on | and then each element must be
     * Base64 decoded , unzipped and deserialized in Trondheim.
     *
     * @param locale        The Locale string, eg. en_be
     * @param uriInfo       The URL of this request, including querystring which has all Custommeta params. Auto filled by JAX
     * @param encodedParams The actual custom meta query parameters
     * @param templateId
     * @return a Base64 encoded, GZipped String of dynamic components, seperated by |.
     */
    @GET
    @Path ("/getcomponentsbycustommeta/{locale}/{params}/{templateid}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getComponentsByCustomMeta(@PathParam ("locale") final String locale,
                                            @Context UriInfo uriInfo,
                                            @PathParam ("params") final String encodedParams,
                                            @PathParam ("templateid") final int templateId,
                                            @Context HttpServletRequest request) {
        // The query params are also base64 encoded, read the params and decode them
        if (encodedParams != null && !encodedParams.isEmpty()) {
            try {
                // Decode the queryString
                String decodedValue = decodeUrl(encodedParams);




                UriInfo decodedUriInfo = new ResteasyUriInfo("",decodedValue,"");
                return TridionCustomMetaQueryProvider.INSTANCE.getComponentsByCustomMeta(locale,
                        decodedUriInfo.getQueryParameters(), templateId);

            } catch (ParseException | StorageException | IOException e) {
                LOG.error(e.getMessage(), e);
            } catch (ItemNotFoundException e) {
                LOG.info("Something was not found: {}", e.getMessage());
            }
        }

        return null;
    }

    @GET
    @Path ("/getcomponentsbyschema/{locale}/{schema}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getComponentPresentationsBySchema(@PathParam ("locale") final String locale,
                                                    @PathParam ("schema") final String schema,
                                                    @Context HttpServletRequest request) {
        try {
            String decodedSchema = decodeUrl(schema);
            return TridionCustomMetaQueryProvider.INSTANCE.getComponentsBySchema(locale, decodedSchema, 0);
        } catch (ParseException | StorageException | IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (ItemNotFoundException e) {
            LOG.info("Something wasn't found: {}", e.getMessage());
        }

        return null;
    }

    @GET
    @Path ("/getcomponentsbyschema/{locale}/{schema}/{templateid}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getComponentPresentationsBySchema(@PathParam ("locale") final String locale,
                                                    @PathParam ("schema") final String schema,
                                                    @DefaultValue ("0") @PathParam ("templateid") final int templateId,
                                                    @Context HttpServletRequest request) {
        try {
            String decodedSchema = decodeUrl(schema);
            return TridionCustomMetaQueryProvider.INSTANCE.getComponentsBySchema(locale, decodedSchema, templateId);
        } catch (ParseException | StorageException | IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (ItemNotFoundException e) {
            LOG.info("Something wasn't found: {}", e.getMessage());
        }

        return null;
    }

	@GET
	@Path ("/getcomponentsbyschemainkeyword/{locale}/{schema}/{categoryid}/{keywordid}/{templateid}")
	@Produces (MediaType.TEXT_PLAIN)
	public String getComponentPresentationsBySchemaInKeyword(@PathParam ("locale") final String locale,
	                                                         @PathParam ("schema") final String schema,
	                                                         @PathParam ("categoryid") final int categoryId,
	                                                         @PathParam ("keywordid") final int keywordId,
	                                                         @DefaultValue ("0") @PathParam ("templateid") final int templateId) {
		try {
			String decodedSchema = decodeUrl(schema);
			return TridionCustomMetaQueryProvider.INSTANCE.getComponentsBySchemaInKeyword(locale, decodedSchema,categoryId,keywordId, templateId);
		} catch (ParseException | StorageException | IOException e) {
			LOG.error(e.getMessage(), e);
		} catch (ItemNotFoundException e) {
			LOG.info("Something wasn't found or died: {}", e.getMessage());
		}
		return null;
	}
    @GET
    @Path ("/getvaluesforcustommetakey/{locale}/{metakey}")
    @Produces (MediaType.TEXT_PLAIN)
    public String getValuesForCustomMetaKey(@PathParam ("locale") final String locale, @PathParam ("metakey") final String metakey) {

        try {
            final String decodedMetaKey = decodeUrl(metakey);
            return TridionCustomMetaQueryProvider.INSTANCE.getValuesForCustomMetaKey(locale,decodedMetaKey);
        } catch (ParseException | ItemNotFoundException | StorageException e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }


    //TODO remove this duplicated code block
    private String decodeUrl(final String url) throws ItemNotFoundException {
        if (url != null) {
            String decodedUrl = new String(urlCoder.decode(url));
            LOG.debug("Decoded Url: {} ", decodedUrl);
            return decodedUrl;
        }

        throw new ItemNotFoundException("Url parameter could not be decoded. Item not found or parameter was null.");
    }
}
