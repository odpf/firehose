package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * JsonWrappedProto wrap encoded64 protobuff message into json format. The
 * format would look like
 * <pre>
 * {
 *   "topic":"sample-topic",
 *   "log_key":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d",
 *   "log_message":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM\u003d"
 * }
 * </pre>
 */
public class JsonWrappedProtoByte implements MessageSerializer {

  private Gson gson;

  /**
   * Instantiates a new Json wrapped proto byte.
   */
  public JsonWrappedProtoByte() {
    this.gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageJsonSerializer()).create();
  }

  /**
   * Serialize string.
   *
   * @param message the message
   * @return the string
   * @throws DeserializerException the deserializer exception
   */
  @Override
  public String serialize(Message message) throws DeserializerException {
    return gson.toJson(message);
  }

}
