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
public class GrpcResponseCELPayloadEvaluator implements PayloadEvaluator<Message> {

    private final String celExpression;
    private Script script;

    public GrpcResponseCELPayloadEvaluator(Descriptors.Descriptor descriptor, String celExpression) {
        this.celExpression = celExpression;
        this.script = buildScript(descriptor);
    }

    @Override
    public boolean evaluate(Message payload) {
        try {
            Map<String, Object> arguments = new HashMap<>();
            arguments.put(payload.getDescriptorForType().getFullName(), payload);
            return this.script.execute(Boolean.class, arguments);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(
                    "Failed to evaluate payload with CEL Expression with reason: " + e.getMessage(), e);
        }
    }

    private Script buildScript(Descriptors.Descriptor payloadDescriptor) {
        try {
            log.info("Building new CEL Script");
            return ScriptHost.newBuilder()
                    .build()
                    .buildScript(this.celExpression)
                    .withDeclarations(Decls.newVar(payloadDescriptor.getFullName(), Decls.newObjectType(payloadDescriptor.getFullName())))
                    .withTypes(DynamicMessage.newBuilder(payloadDescriptor).getDefaultInstanceForType())
                    .build();
        } catch (ScriptCreateException e) {
            throw new IllegalArgumentException("Failed to build CEL Script due to : " + e.getMessage(), e);
        }
    }
}
