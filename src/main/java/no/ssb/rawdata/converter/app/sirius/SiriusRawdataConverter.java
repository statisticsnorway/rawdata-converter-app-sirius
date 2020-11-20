package no.ssb.rawdata.converter.app.sirius;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import no.ssb.avro.convert.xml.XmlToRecords;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.sirius.schema.SchemaAdapter;
import no.ssb.rawdata.converter.app.sirius.schema.SiriusSchemas;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ConversionResult.ConversionResultBuilder;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.schema.AggregateSchemaBuilder;
import no.ssb.rawdata.converter.core.schema.DcManifestSchemaAdapter;
import no.ssb.rawdata.converter.util.AvroSchemaUtil;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import no.ssb.rawdata.converter.util.Xml;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

@Slf4j
public class SiriusRawdataConverter implements RawdataConverter {

    private static final String FIELDNAME_MANIFEST = "manifest";
    private static final String FIELDNAME_DC_MANIFEST = "collector";
    private static final String FIELDNAME_CONVERTER_MANIFEST = "converter";

    private final SiriusRawdataConverterConfig converterConfig;
    private final ValueInterceptorChain valueInterceptorChain;
    private final Set<String> requiredRawdataItems;

    private final Schema converterManifestSchema;
    private DcManifestSchemaAdapter dcManifestSchemaAdapter;
    private Schema manifestSchema;
    private final Set<SchemaAdapter> dataSchemas;
    private Schema targetAvroSchema;

    public SiriusRawdataConverter(SiriusRawdataConverterConfig converterConfig, ValueInterceptorChain valueInterceptorChain) {
        this.converterConfig = converterConfig;
        this.valueInterceptorChain = valueInterceptorChain;
        this.converterManifestSchema = AvroSchemaUtil.readAvroSchema("schema/converter-manifest.avsc");
        this.dataSchemas = converterConfig.getDataElements()
          .stream().map(schemaDescriptor -> SiriusSchemas.getBySchemaDescriptor(schemaDescriptor))
          .collect(Collectors.toSet());
        this.requiredRawdataItems = dataSchemas.stream()
          .filter(schema -> !schema.getOptional())
          .map(schema -> schema.getRawdataItemName())
          .collect(Collectors.toSet());
    }

    @Override
    public void init(Collection<RawdataMessage> sampleRawdataMessages) {
        log.info("Determine target avro schema from {}", sampleRawdataMessages);
        RawdataMessage sample = sampleRawdataMessages.stream()
          .findFirst()
          .orElseThrow(() ->
            new SiriusRawdataConverterException("Unable to determine target avro schema since no sample rawdata messages were supplied. Make sure to configure `converter-settings.rawdata-samples`")
          );

        RawdataMessageAdapter msg = new RawdataMessageAdapter(sample);
        dcManifestSchemaAdapter = DcManifestSchemaAdapter.of(sample);

        manifestSchema = new AggregateSchemaBuilder("dapla.rawdata.manifest")
          .schema("collector", dcManifestSchemaAdapter.getDcManifestSchema())
          .schema("converter", converterManifestSchema)
          .build();

        String targetNamespace = "dapla.rawdata.ske.skatt." + msg.getTopic().orElse("dataset");
        AggregateSchemaBuilder targetSchemaBuilder = new AggregateSchemaBuilder(targetNamespace)
          .schema("manifest", manifestSchema)
          .schema(FIELDNAME_DC_MANIFEST, dcManifestSchemaAdapter.getDcManifestSchema())
          .schema(FIELDNAME_CONVERTER_MANIFEST, converterManifestSchema);

        dataSchemas.forEach(schema -> {
            targetSchemaBuilder.schema(schema.getTargetItemName(), schema.getSchema());
        });

        targetAvroSchema = targetSchemaBuilder.build();
    }

    public DcManifestSchemaAdapter dcManifestSchemaAdapter() {
        if (dcManifestSchemaAdapter == null) {
            throw new IllegalStateException("dcManifestSchemaAdapter is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return dcManifestSchemaAdapter;
    }

    @Override
    public Schema targetAvroSchema() {
        if (targetAvroSchema == null) {
            throw new IllegalStateException("targetAvroSchema is null. Make sure RawdataConverter#init() was invoked in advance.");
        }

        return targetAvroSchema;
    }

    // TODO: Use XPath instead
    @Override
    public boolean isConvertible(RawdataMessage rawdataMessage) {
        // Skip messages that does not contain all required items
        if (! rawdataMessage.keys().containsAll(requiredRawdataItems)) {
            log.warn("Missing required rawdata items {}. Skipping rawdataMessage {}", Sets.difference(requiredRawdataItems, rawdataMessage.keys()), posAndIdOf(rawdataMessage));
            return false;
        }

        RawdataMessageAdapter msg = new RawdataMessageAdapter(rawdataMessage);

        // TODO: Move this to a validation method associated with each schema adapter instead?

        // Skip messages that does not match "hendelse.gjelderPeriode"
        if (msg.hasItem("entry")) {
            Map<String, Object> hendelse = Xml.toGenericMap(msg.getItemAsString("entry"));
            String gjelderPeriode = (String) hendelse.get("gjelderPeriode");
            if (! gjelderPeriode.equals(converterConfig.getPeriod())) {
                log.info("Rawdata gjelderPeriode={} is not compatible with period {}. Skipping rawdata item {}", gjelderPeriode, converterConfig.getPeriod(), posAndIdOf(rawdataMessage));
                return false;
            }
        }
        else {
            log.info("No hendelse item found. Skipping rawdata item {}", posAndIdOf(rawdataMessage));
            return false;
        }
/*
        // Skip messages with screened skattemelding-records // TODO: Include these somehow
        if (msg.hasItem("skattemelding")) {
            Map<String, Object> skattemelding = Xml.toGenericMap(msg.getItemAsString("skattemelding"));
            if ("true".equals(skattemelding.get("skjermet"))) {
                log.info("Skattemelding is marked as being 'skjermet'. Skipping rawdata item {}", posAndIdOf(rawdataMessage));
                return false;
            }
        }
*/
        return true;
    }

    @Override
    public ConversionResult convert(RawdataMessage rawdataMessage) {
        ConversionResultBuilder resultBuilder = ConversionResult.builder(targetAvroSchema, rawdataMessage);

        addManifest(rawdataMessage, resultBuilder);
        dataSchemas.forEach(schema -> {
            if (rawdataMessage.keys().contains(schema.getRawdataItemName())) {
                convertXml(rawdataMessage, resultBuilder, schema);
            }
        });

        return resultBuilder.build();
    }

    void addManifest(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder) {
        GenericRecord manifest = new GenericRecordBuilder(manifestSchema)
          .set(FIELDNAME_DC_MANIFEST, dcManifestSchemaAdapter().newRecord(rawdataMessage, valueInterceptorChain))
          .set(FIELDNAME_CONVERTER_MANIFEST, converterManifestData())
          .build();

        resultBuilder.withRecord(FIELDNAME_MANIFEST, manifest);
    }

    GenericRecord converterManifestData() {
        Map<String, String> schemaInfo = dataSchemas.stream()
          .collect(Collectors.toMap(
            SchemaAdapter::getTargetItemName,
            SchemaAdapter::getSchemaName
          ));

        return new GenericRecordBuilder(converterManifestSchema)
          .set("schemas", schemaInfo)
          .build();
    }

    void convertXml(RawdataMessage rawdataMessage, ConversionResultBuilder resultBuilder, SchemaAdapter schemaAdapter) {
        byte[] data = rawdataMessage.get(schemaAdapter.getRawdataItemName());
        try (XmlToRecords records = new XmlToRecords(new ByteArrayInputStream(data), schemaAdapter.getRootElementName(), schemaAdapter.getSchema(), valueInterceptorChain)) {
            records.forEach(record ->
              resultBuilder.withRecord(schemaAdapter.getTargetItemName(), record)
            );
        }
        catch (Exception e) {
            throw new SiriusRawdataConverterException("Error converting sirius " + schemaAdapter.getRawdataItemName() + " data at " + posAndIdOf(rawdataMessage), e);
        }
    }

    public static class SiriusRawdataConverterException extends RawdataConverterException {
        public SiriusRawdataConverterException(String msg) {
            super(msg);
        }
        public SiriusRawdataConverterException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}