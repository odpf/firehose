package com.gotocompany.firehose.sink.http.request.body;

import java.util.ArrayList;
import java.util.List;

import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageSerializer;

/**
 * JsonBody Serialize the message according to injected serialzier and return it
 * as List of serialized string.
 */
public class JsonBody {

  private MessageSerializer jsonSerializer;

  /**
   * Instantiates a new Json body.
   *
   * @param jsonSerializer the json serializer
   */
  public JsonBody(MessageSerializer jsonSerializer) {
    this.jsonSerializer = jsonSerializer;
  }

  /**
   * Serialize list.
   *
   * @param messages the messages
   * @return the list
   * @throws DeserializerException the deserializer exception
   */
  public List<String> serialize(List<Message> messages) throws DeserializerException {
    List<String> serializedBody = new ArrayList<String>();
    for (Message message : messages) {
      serializedBody.add(jsonSerializer.serialize(message));
    }
    return serializedBody;
  }

}
