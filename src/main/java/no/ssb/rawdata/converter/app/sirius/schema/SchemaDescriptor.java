package no.ssb.rawdata.converter.app.sirius.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This allows for overriding properties in a SchemaAdapter
 * TODO: Don't duplicate XmlSchemaAdapter
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SchemaDescriptor {
    public SchemaDescriptor(String schemaName) {
        this.schemaName = schemaName;
    }

    private String schemaName;
    private Boolean optional;
    private String rawdataItemName;
    private String targetItemName;
    private String rootElementName;
}
