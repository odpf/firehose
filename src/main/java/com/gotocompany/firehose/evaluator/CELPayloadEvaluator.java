package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class CELPayloadEvaluator implements PayloadEvaluator<Message> {

    private final Script script;

    public CELPayloadEvaluator(Descriptors.Descriptor descriptor, String celExpression) {
        try {
            script = ScriptHost.newBuilder()
                    .build()
                    .buildScript(celExpression)
                    .withDeclarations(Decls.newVar(descriptor.getFullName(), Decls.newObjectType(descriptor.getFullName())))
                    .withTypes(DynamicMessage.newBuilder(descriptor).getDefaultInstanceForType())
                    .build();
        } catch (ScriptCreateException e) {
            throw new IllegalArgumentException("Failed to build CEL Script with reason: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean evaluate(Message payload) {
        try {
            Map<String, Object> arguments = new HashMap<>();
            arguments.put(payload.getDescriptorForType().getFullName(), payload);
            return script.execute(Boolean.class, arguments);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(
                    "Failed to evaluate payload with CEL Expression with reason: " + e.getMessage(), e);
        }
    }

}
