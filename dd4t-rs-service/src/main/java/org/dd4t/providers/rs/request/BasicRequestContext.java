package org.dd4t.providers.rs.request;

import org.dd4t.core.request.RequestContext;

import javax.servlet.http.HttpServletRequest;

/**
 * @author Mihai Cadariu
 * @since 04.07.2014
 */
public class BasicRequestContext implements RequestContext {

    private HttpServletRequest request;

    public BasicRequestContext(HttpServletRequest request) {
        this.request = request;
    }

    @Override
    public Object getRequest () {
        return request;
    }
}
