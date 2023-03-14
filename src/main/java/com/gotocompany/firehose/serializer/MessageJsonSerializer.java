package com.gotocompany.firehose.serializer;

import com.gotocompany.firehose.message.Message;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Message Json serializer serialises Kafka message to json.
 */
public class MessageJsonSerializer implements JsonSerializer<Message> {

    /**
     * Serialize kafka message into json element.
     *
     * @param message   the message
     * @param typeOfSrc the type of src
     * @param context   the context
     * @return the json element
     */
    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject object = new JsonObject();
        object.addProperty("topic", message.getTopic());
        object.addProperty("log_key", message.getSerializedKey());
        object.addProperty("log_message", message.getSerializedMessage());
        return object;
    }
}
