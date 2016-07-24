package org.dd4t.providers.rs;

import org.apache.commons.codec.binary.Base64;
import org.dd4t.providers.PayloadCacheProvider;

import javax.annotation.Resource;

/**
 * dd4t-2-rs-service
 *
 * @author R. Kempees
 */
public abstract class TridionBaseProvider {

    @Resource(name = "cacheProvider")
    protected PayloadCacheProvider cacheProvider;

    protected final Base64 urlCoder = new Base64(true);

    public PayloadCacheProvider getCacheProvider () {
        return cacheProvider;
    }

    public void setCacheProvider (final PayloadCacheProvider cacheProvider) {
        this.cacheProvider = cacheProvider;
    }

    public Base64 getUrlCoder () {
        return urlCoder;
    }
}
