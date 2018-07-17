import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.cementownia.playground.avro.Broken;
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


   private Broken deserialize(byte[] bytes) throws IOException {
      SpecificDatumReader<Broken> reader = new SpecificDatumReader<Broken>(Broken.getClassSchema());
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
}

