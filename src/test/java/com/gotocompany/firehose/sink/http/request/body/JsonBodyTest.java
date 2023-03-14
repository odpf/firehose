package com.gotocompany.firehose.sink.http.request.body;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageSerializer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JsonBodyTest {

  @Mock
  private MessageSerializer messageSerializer;

  private Message message;
  private List<Message> messages;

  @Before
  public void setUp() {
    message = new Message(new byte[] {10, 20 }, new byte[] {1, 2 }, "sample-topic", 0, 100);
    messages = Collections.singletonList(message);
  }

  @Test
  public void shouldReturnSameSizeOfBodyAsEsbMessage() {
    JsonBody jsonBody = new JsonBody(messageSerializer);

    List<String> bodyContent;
    try {
      bodyContent = jsonBody.serialize(messages);
    } catch (DeserializerException e) {
      throw new RuntimeException(e.toString());
    }
    assertEquals(1, bodyContent.size());
  }

  @Test
  public void shouldReturnSerializedValueOfMessage() {
    List<String> contentString;
    String mockSerializeResult = "{\"MockSerializer\": []}";
    try {
      when(messageSerializer.serialize(message)).thenReturn(mockSerializeResult);

      JsonBody jsonBody = new JsonBody(messageSerializer);
      contentString = jsonBody.serialize(messages);

    } catch (DeserializerException e) {
      throw new RuntimeException(e.toString());
    }

    assertEquals(mockSerializeResult, contentString.get(0));
  }
}
