package org.dd4t.providers.serializer;

import com.tridion.storage.BinaryVariant;
import com.tridion.storage.ComponentMeta;
import com.tridion.storage.CustomMetaValue;
import org.dd4t.contentmodel.Binary;
import org.dd4t.contentmodel.OrganizationalItem;
import org.dd4t.contentmodel.Publication;
import org.dd4t.contentmodel.Schema;
import org.dd4t.contentmodel.impl.BinaryImpl;
import org.dd4t.contentmodel.impl.OrganizationalItemImpl;
import org.dd4t.contentmodel.impl.PublicationImpl;
import org.dd4t.contentmodel.impl.SchemaImpl;
import org.dd4t.core.util.TCMURI;
import org.joda.time.DateTime;

import java.util.HashMap;

/**
 * Builds a Binary object from a BinaryVariant. Maps all existing fields from BinaryVariant into their respective
 * Binary couterparts.
 *
 * @author Mihai Cadariu
 * @since 10.06.2014
 */
public class BinaryBuilder {

    public Binary build(BinaryVariant variant) {
        Binary result = new BinaryImpl();

        //result.setAlt();
        result.setId(buildId(variant));
        result.setCustomProperties(buildCustomProperties(variant));
        //result.setHeight();
        ComponentMeta multimediaMeta = variant.getBinaryMeta().getMultimediaMeta();
        result.setLastPublishDate(new DateTime(multimediaMeta.getLastPublishDate()));
        result.setMimeType(variant.getBinaryType());
        result.setOrganizationalItem(buildOrganizationalItem(variant));
        result.setOwningPublication(buildOwningPublication(variant));
        result.setPublication(buildPublication(variant));
        result.setSchema(buildSchema(variant));
        result.setTitle(multimediaMeta.getTitle());
        //result.setWidth();

        return result;
    }

    private String buildId(BinaryVariant variant) {
        return new TCMURI(variant.getPublicationId(), variant.getBinaryId(), 16, 0).toString();
    }

    private HashMap<String, Object> buildCustomProperties(BinaryVariant variant) {
        HashMap<String, Object> result = new HashMap<String, Object>();

        for (CustomMetaValue metaValue : variant.getBinaryMeta().getMultimediaMeta().getCustomMetaValues()) {
            result.put(metaValue.getKeyName(), metaValue.getValue());
        }

        return result;
    }

    private OrganizationalItem buildOrganizationalItem(BinaryVariant variant) {
        OrganizationalItemImpl result = null;
        String structureGroupId = variant.getStructureGroupId();

        if (structureGroupId != null) {
            result = new OrganizationalItemImpl();

            //result.setCustomProperties();
            result.setId(structureGroupId);
            result.setPublicationId(new TCMURI(0, variant.getPublicationId(), 1, 0).toString());
            //result.setTitle();
        }

        return result;
    }

    private Publication buildOwningPublication(BinaryVariant variant) {
        Publication result = null;
        Integer owningPublication = variant.getBinaryMeta().getMultimediaMeta().getOwningPublication();

        if (owningPublication != 0) {
            result = new PublicationImpl();
            //result.setCustomProperties();
            result.setId(new TCMURI(0, owningPublication, 1, 0).toString());
            //result.setTitle();
        }

        return result;
    }

    private Publication buildPublication(BinaryVariant variant) {
        Publication result = new PublicationImpl();

        //result.setCustomProperties();
        result.setId(new TCMURI(0, variant.getPublicationId(), 1, 0).toString());
        //result.setTitle();

        return result;
    }

    private Schema buildSchema(BinaryVariant variant) {
        SchemaImpl result = null;
        Integer schemaId = variant.getBinaryMeta().getMultimediaMeta().getSchemaId();

        if (schemaId != 0) {
            result = new SchemaImpl();

            //result.setCustomProperties();
            result.setId(new TCMURI(variant.getPublicationId(), schemaId, 8, 0).toString());
            //result.setPublication();
            //result.setOwningPublication();
            //result.setOrganizationalItem();
            //result.setRootElement();
            //result.setTitle();
        }

        return result;
    }
}
