package com.gotocompany.firehose.filter;

import com.gotocompany.firehose.message.Message;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
@EqualsAndHashCode
public class FilteredMessages {
    private final List<Message> validMessages = new ArrayList<>();
    private final List<Message> invalidMessages = new ArrayList<>();

    public void addToValidMessages(Message message) {
        validMessages.add(message);
    }

    public void addToInvalidMessages(Message message) {
        invalidMessages.add(message);
    }

    public int sizeOfValidMessages() {
        return validMessages.size();
    }

    public int sizeOfInvalidMessages() {
        return invalidMessages.size();
    }
}


