import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.cementownia.playground.avro.Broken;
import org.cementownia.playground.avro.BrokenTestStatus;
import org.cementownia.playground.avro.BrokenTestStatusV2;
import org.cementownia.playground.avro.BrokenV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BrokenTests {
   private byte[] serialize(Broken broken) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<Broken> writer = new SpecificDatumWriter<>(Broken.getClassSchema());

      writer.write(broken, encoder);
      encoder.flush();
      out.close();
      return out.toByteArray();
   }

   private byte[] serialize(BrokenV2 broken) throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<BrokenV2> writer = new SpecificDatumWriter<>(BrokenV2.getClassSchema());

      writer.write(broken, encoder);
      encoder.flush();
      out.close();
      return out.toByteArray();
   }


   private Broken deserializeFromV2(byte[] bytes) throws IOException {
      String v2Schema = BrokenV2.getClassSchema().toString().replaceAll("V2", "");
      Schema.Parser parser = new Schema.Parser();
      Schema v2 = parser.parse(v2Schema);

      SpecificDatumReader<Broken> reader = new SpecificDatumReader<>(v2, Broken.getClassSchema());
      Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
      return reader.read(null, decoder);
   }

   @Test
   public void shouldThrownAnExceptionDueToWrongAvro() {
      // given
      Broken.Builder builder = Broken.newBuilder()
            .setId("1234");

      // when
      Throwable thrown = Assertions.assertThrows(AvroRuntimeException.class, () -> builder.build());

      // then
      Assertions.assertTrue(thrown.getMessage().contains("Field status type:ENUM pos:0 not set and has no default value"));
   }

   @Test
   public void shouldDeserializeV2WithOkEnumToV1WithOkEnum() throws IOException {
      // given
      BrokenV2 v2 = BrokenV2.newBuilder()
            .setId("1234")
            .setStatus(BrokenTestStatusV2.OK)
            .build();

      // when
      byte[] bytes = serialize(v2);
      Broken deserialized = deserializeFromV2(bytes);

      // then
      Broken expected = Broken.newBuilder()
            .setId("1234")
            .setStatus(BrokenTestStatus.OK)
            .build();

      Assertions.assertEquals(expected, deserialized);
   }

   @Test
   public void shouldDeserializeV2WithNewEnumToV1WithDefaultEnum() throws IOException {
      // given
      BrokenV2 v2 = BrokenV2.newBuilder()
            .setId("1234")
            .setStatus(BrokenTestStatusV2.NEW)
            .build();

      // when
      byte[] bytes = serialize(v2);
      Broken deserialized = deserializeFromV2(bytes);

      // then
      Broken expected = Broken.newBuilder()
            .setId("1234")
            .setStatus(BrokenTestStatus.FAIL)
            .build();

      Assertions.assertEquals(expected, deserialized);
   }
}

