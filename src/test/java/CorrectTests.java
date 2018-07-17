import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.cementownia.playground.avro.Correct;
import org.cementownia.playground.avro.CorrectTestStatus;
import org.cementownia.playground.avro.CorrectTestStatusV2;
import org.cementownia.playground.avro.CorrectV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CorrectTests {
    private byte[] serialize(Correct correct) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Correct> writer = new SpecificDatumWriter<>(Correct.getClassSchema());

        writer.write(correct, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    private byte[] serialize(CorrectV2 correct) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<CorrectV2> writer = new SpecificDatumWriter<>(CorrectV2.getClassSchema());

        writer.write(correct, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    private Correct deserializeFromV2(byte[] bytes) throws IOException {
        /*
        We have to be really creative here, because we're using two schema versions.
        Java generator will ignore conflicts for the same classess in the same namespace,
        thus generating StatusEnum whichever comes last...so I had to use different names.
        Different names though won't match, so we have to strip the V2 from avro schema.

        The more you know...
         */
        String v2Schema = CorrectV2.getClassSchema().toString().replaceAll("V2", "");
        Schema.Parser parser = new Schema.Parser();
        Schema v2 = parser.parse(v2Schema);

        SpecificDatumReader<Correct> reader = new SpecificDatumReader<>(v2, Correct.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, decoder);
    }

    @Test
    public void shouldDefaultToStatusOk() {
        // given
        Correct avroTest = Correct.newBuilder()
              .setId("1234")
              .build();

        // then
        Assertions.assertEquals(CorrectTestStatus.OK, avroTest.getStatus());
    }

    @Test
    public void shouldDeserializeV2WithOkEnumToV1WithOkEnum() throws IOException {
        // given
        CorrectV2 v2 = CorrectV2.newBuilder()
              .setId("1234")
              .setStatus(CorrectTestStatusV2.OK)
              .build();

        // when
        byte[] bytes = serialize(v2);
        Correct deserialized = deserializeFromV2(bytes);

        // then
        Correct expected = Correct.newBuilder()
              .setId("1234")
              .setStatus(CorrectTestStatus.OK)
              .build();

        Assertions.assertEquals(expected, deserialized);
    }

    @Test
    public void shouldDeserializeV2WithNewEnumToV1WithDefaultEnum() throws IOException {
        // given
        CorrectV2 v2 = CorrectV2.newBuilder()
              .setId("1234")
              .setStatus(CorrectTestStatusV2.NEW)
              .build();

        // when
        byte[] bytes = serialize(v2);
        Correct deserialized = deserializeFromV2(bytes);

        // then
        Correct expected = Correct.newBuilder()
              .setId("1234")
              .setStatus(CorrectTestStatus.OK)
              .build();

        Assertions.assertEquals(expected, deserialized);
    }
}
