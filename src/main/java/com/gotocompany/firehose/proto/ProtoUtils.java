package com.gotocompany.firehose.proto;

import com.google.protobuf.DynamicMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class ProtoUtils {
    public static boolean hasUnknownField(DynamicMessage root) {
        List<DynamicMessage> dynamicMessageFields = collectNestedFields(root);
        List<DynamicMessage> messageWithUnknownFields = getMessageWithUnknownFields(dynamicMessageFields);
        return messageWithUnknownFields.size() > 0;
    }

    private static List<DynamicMessage> collectNestedFields(DynamicMessage node) {
        List<DynamicMessage> output = new LinkedList<>();
        Queue<DynamicMessage> stack = Collections.asLifoQueue(new LinkedList<>());
        stack.add(node);
        while (true) {
            DynamicMessage current = stack.poll();
            if (current == null) {
                break;
            }
            List<DynamicMessage> nestedChildNodes = current.getAllFields().values().stream()
                    .filter(field -> field instanceof DynamicMessage)
                    .map(field -> (DynamicMessage) field)
                    .collect(Collectors.toList());
            stack.addAll(nestedChildNodes);

            output.add(current);
        }

        return output;
    }

    private static List<DynamicMessage> getMessageWithUnknownFields(List<DynamicMessage> messages) {
        return messages.stream().filter(message -> message.getUnknownFields().asMap().size() > 0).collect(Collectors.toList());
    }
}
