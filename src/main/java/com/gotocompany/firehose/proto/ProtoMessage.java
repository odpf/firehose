package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.message.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;

import java.lang.reflect.Method;

public class ProtoMessage {
    public static final String CLASS_NAME_NOT_FOUND = "proto class provided in the configuration was not found";
    public static final String INVALID_PROTOCOL_CLASS_MESSAGE = "Invalid proto class provided in the configuration";
    public static final String DESERIALIZE_ERROR_MESSAGE = "Esb message could not be parsed";
    private Method messageParser;

    public ProtoMessage(String protoClassName) {
        this.messageParser = parserMethod(protoClassName);
    }

    public Object get(Message message, int protoIndex) throws DeserializerException {
        GeneratedMessageV3 protoMsg;
        protoMsg = (GeneratedMessageV3) parseProtobuf(message);
        Descriptors.FieldDescriptor fieldDescriptor = protoMsg.getDescriptorForType().findFieldByNumber(protoIndex);
        return protoMsg.getField(fieldDescriptor);
    }

    public Object parseProtobuf(Message message) throws DeserializerException {
        try {
            return messageParser.invoke(null, message.getLogMessage());
        } catch (ReflectiveOperationException e) {
            throw new DeserializerException(DESERIALIZE_ERROR_MESSAGE, e);
        }
    }

    private Method parserMethod(String protoClassName) {
        Class<com.google.protobuf.Message> builderClass;
        try {
            builderClass = (Class<com.google.protobuf.Message>) Class.forName(protoClassName);
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException(CLASS_NAME_NOT_FOUND, e);
        }
        try {
            return builderClass.getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new ConfigurationException(INVALID_PROTOCOL_CLASS_MESSAGE, e);
        }
    }
}
