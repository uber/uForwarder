package com.uber.data.kafka.consumerproxy.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.springframework.boot.context.event.ApplicationReadyEvent;

public class ApplicationReadyListenerTest extends FievelTestBase {
  @Test
  public void testOnApplicationEvent() {
    Logger logger = Mockito.mock(Logger.class);
    ApplicationReadyListener listener =
        ApplicationReadyListener.newBuilder("testApp").withLogger(logger).build();
    listener.onApplicationEvent(Mockito.mock(ApplicationReadyEvent.class));
    Mockito.verify(logger).info("{} started successfully", "testApp");
  }
}
