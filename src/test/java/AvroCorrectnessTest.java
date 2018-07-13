import org.apache.avro.AvroRuntimeException;
import org.cementownia.playground.avro.Broken;
import org.cementownia.playground.avro.Correct;
import org.cementownia.playground.avro.CorrectTestStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvroCorrectnessTest {
    @Test
    public void shouldThrownAnExceptionDueToWrongAvro() {
        // given
        Broken.Builder builder = Broken.newBuilder();

        // when
        Throwable thrown = Assertions.assertThrows(AvroRuntimeException.class, () -> builder.build());

        // then
        Assertions.assertTrue(thrown.getMessage().contains("Field status type:ENUM pos:0 not set and has no default value"));
    }

    @Test
    public void shouldDefaultToStatusOk() {
        // given
        Correct avroTest = Correct.newBuilder().build();

        // then
        Assertions.assertEquals(CorrectTestStatus.OK, avroTest.getStatus());
    }
}
