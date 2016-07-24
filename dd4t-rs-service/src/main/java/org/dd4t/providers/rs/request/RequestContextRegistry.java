package org.dd4t.providers.rs.request;

import org.dd4t.core.request.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mihai Cadariu
 * @since 04.07.2014
 */
public class RequestContextRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(RequestContextRegistry.class);
    private static RequestContextRegistry _instance = new RequestContextRegistry();
    private Map<String, RequestContext> registry = new HashMap<>();

    private RequestContextRegistry() {
    }

    public static RequestContext getCurrentRequestContext() {
        String id = getCurrentRequestId();
        RequestContext result = _instance.registry.get(id);
        LOG.debug("Return RequestContext: {} for id: {}", result, id);
        return result;
    }

    public static void setCurrentRequestContext(RequestContext requestContext) {
        String id = getCurrentRequestId();
        LOG.debug("Set RequestContext: {} for id: {}", requestContext, id);
        _instance.registry.put(id, requestContext);
    }

    public static void removeCurrentRequestContext() {
        String id = getCurrentRequestId();
        RequestContext requestContext = _instance.registry.remove(id);
        LOG.debug("Remove RequestContext: {} for id: {}", requestContext, id);
    }

    private static String getCurrentRequestId() {
        return Thread.currentThread().getName();
    }
}
