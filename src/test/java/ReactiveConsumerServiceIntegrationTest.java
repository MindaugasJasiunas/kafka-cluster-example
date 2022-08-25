import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(topics = {"${FAKE_MESSAGEPOJO_TOPIC}", "${FAKE_MESSAGEPOJO_TOPIC}"})
class ReactiveConsumerServiceIntegrationTest {

    @Test
    void reactiveConsumerService_receiveMessagePojoFromTopic_saveMessagePojo() {
        // TODO
    }

}