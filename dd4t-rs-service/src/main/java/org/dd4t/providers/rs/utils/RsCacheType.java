package org.dd4t.providers.rs.utils;

/**
 * dd4t-2-rs-service
 *
 * @author R. Kempees
 */
public enum RsCacheType {
    CUSTOM_META_VALUES_FOR_KEY("CMVK");

    private String id;

    /**
     * Initialization constructor
     *
     * @param id String representing the value of the enumeration value
     */
    RsCacheType(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return id;
    }
}
