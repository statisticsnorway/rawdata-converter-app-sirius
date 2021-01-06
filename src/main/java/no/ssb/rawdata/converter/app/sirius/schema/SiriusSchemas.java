package no.ssb.rawdata.converter.app.sirius.schema;

import no.ssb.rawdata.converter.app.sirius.SiriusRawdataConverter.SiriusRawdataConverterException;

import java.util.Set;

import static no.ssb.rawdata.converter.util.AvroSchemaUtil.readAvroSchema;

public class SiriusSchemas {

    private static final Set<SchemaAdapter> SCHEMAS;

    static {
        SCHEMAS = Set.of(
          SchemaAdapter.builder()
            .schemaName("sirius-hendelse-v1_3")
            .schema(readAvroSchema("schema/sirius-hendelse-v1_3.avsc"))
            .rawdataItemName("entry")
            .targetItemName("hendelse")
            .rootElementName("hendelse")
            .build(),
          SchemaAdapter.builder()
            .schemaName("sirius-skattemelding-utflatet-2018-v0_22")
            .schema(readAvroSchema("schema/sirius-skattemelding-utflatet-2018-v0_22.avsc"))
            .rawdataItemName("skattemelding")
            .targetItemName("skattemeldingUtflatet")
            .rootElementName("skattemeldingUtflatet")
            .build(),
          SchemaAdapter.builder()
            .schemaName("sirius-skattemelding-utflatet-2019-v1_0_1")
            .schema(readAvroSchema("schema/sirius-skattemelding-utflatet-2019-v1_0_1.avsc"))
            .rawdataItemName("skattemelding")
            .targetItemName("skattemeldingUtflatet")
            .rootElementName("skattemeldingUtflatet")
            .build(),
          SchemaAdapter.builder()
            .schemaName("sirius-skattemelding-utflatet-2019-v1_2")
            .schema(readAvroSchema("schema/sirius-skattemelding-utflatet-2019-v1_2.avsc"))
            .rawdataItemName("skattemelding")
            .targetItemName("skattemeldingUtflatet")
            .rootElementName("skattemeldingUtflatet")
            .build(),
          SchemaAdapter.builder()
            .schemaName("sirius-skattemelding-2020-v8_0")
            .schema(readAvroSchema("schema/sirius-skattemelding-2020-v8_0.avsc"))
            .rawdataItemName("skattemelding")
            .targetItemName("skattemelding")
            .rootElementName("skattemelding")
            .build()
        );
    }

    public static SchemaAdapter getBySchemaDescriptor(SchemaDescriptor schemaSource) {
        SchemaAdapter schemaAdapter = SCHEMAS.stream()
          .filter(schema -> schema.getSchemaName().equalsIgnoreCase(schemaSource.getSchemaName()))
          .findFirst()
          .orElseThrow(() ->
            new SchemaNotFoundException("No schema found for " + schemaSource.getSchemaName()));
        schemaAdapter = merge(schemaAdapter, schemaSource);

        return schemaAdapter;
    }

    private static SchemaAdapter merge(SchemaAdapter schemaAdapter, SchemaDescriptor overrides) {
        SchemaAdapter.SchemaAdapterBuilder builder = schemaAdapter.toBuilder();
        if (overrides.getRawdataItemName() != null) {
            builder.rawdataItemName(overrides.getRawdataItemName());
        }
        if (overrides.getTargetItemName() != null) {
            builder.targetItemName(overrides.getTargetItemName());
        }
        if (overrides.getOptional() != null) {
            builder.optional(overrides.getOptional());
        }
        if (overrides.getRootElementName() != null) {
            builder.rootElementName(overrides.getRootElementName());
        }

        return builder.build();
    }

    public static class SchemaNotFoundException extends SiriusRawdataConverterException {
        public SchemaNotFoundException(String msg) {
            super(msg);
        }
    }

}
