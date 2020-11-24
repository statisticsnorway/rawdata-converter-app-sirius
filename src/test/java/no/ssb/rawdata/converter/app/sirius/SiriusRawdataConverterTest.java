package no.ssb.rawdata.converter.app.sirius;

import no.ssb.rawdata.converter.app.sirius.schema.SchemaDescriptor;
import no.ssb.rawdata.converter.core.convert.ConversionResult;
import no.ssb.rawdata.converter.core.convert.ValueInterceptorChain;
import no.ssb.rawdata.converter.test.message.RawdataMessageFixtures;
import no.ssb.rawdata.converter.test.message.RawdataMessages;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class SiriusRawdataConverterTest {

    static RawdataMessageFixtures fixtures;

    @BeforeAll
    static void loadFixtures() {
        fixtures = RawdataMessageFixtures.init("failed-rawdata");
    }

    @Disabled
    @Test
    void shouldConvertRawdataMessages() {
        RawdataMessages messages = fixtures.rawdataMessages("failed-rawdata");
        SiriusRawdataConverterConfig config = new SiriusRawdataConverterConfig();
        config.setPeriod("2019");
        config.setDataElements(Set.of(
          new SchemaDescriptor("sirius-hendelse-v1_3"),
          new SchemaDescriptor("sirius-skattemelding-utflatet-2019-v1_0_1")
        ));

        SiriusRawdataConverter converter = new SiriusRawdataConverter(config, new ValueInterceptorChain());
        converter.init(messages.index().values());
        ConversionResult res = converter.convert(messages.index().get("4336374"));
    }

}
