package no.ssb.rawdata.converter.app.sirius;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;
import lombok.NonNull;
import no.ssb.rawdata.converter.app.sirius.schema.SchemaDescriptor;

import java.util.HashSet;
import java.util.Set;

@ConfigurationProperties("rawdata.converter.sirius")
@Data
public class SiriusRawdataConverterConfig {

    /**
     * Schemas of the expected data elements that the converted data is expected
     * to be compliant with.
     */
    private Set<SchemaDescriptor> dataElements = new HashSet<>();

    /**
     * Only rawdata messages with "hendelse.gjelderPeriode" that matches this
     * property will be converted.
     */
    private String period;

}