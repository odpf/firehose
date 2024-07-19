package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.utils.CelUtils;
import dev.cel.compiler.CelCompiler;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.grpc.Metadata;
import org.apache.commons.collections.MapUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class responsible for mapping Protobuf messages to gRPC metadata using CEL expressions.
 */
public class ProtoToMetadataMapper {

    private static final Pattern CEL_EXPRESSION_MARKER = Pattern.compile("^\\$(.+)");
    private static final int EXACT_CEL_EXPRESSION_GROUP_INDEX = 1;

    private final Map<String, CelRuntime.Program> celExpressionToProgramMap;
    private final Map<String, String> metadataTemplate;
    private final Descriptors.Descriptor descriptor;

    /**
     * Constructor for ProtoToMetadataMapper.
     *
     * @param descriptor       the Protobuf descriptor of the message type
     * @param metadataTemplate a map of metadata keys and values that may contain CEL expressions
     */
    public ProtoToMetadataMapper(Descriptors.Descriptor descriptor, Map<String, String> metadataTemplate) {
        this.metadataTemplate = metadataTemplate;
        this.descriptor = descriptor;
        this.celExpressionToProgramMap = initializeCelPrograms();
    }

    /**
     * Builds gRPC metadata from a Protobuf message in byte array format.
     *
     * @param message the Protobuf message as a byte array
     * @return gRPC metadata
     * @throws DeserializerException if the Protobuf message cannot be parsed
     */
    public Metadata buildGrpcMetadata(byte[] message) {
        if (MapUtils.isEmpty(metadataTemplate)) {
            return new Metadata();
        }
        try {
            return buildGrpcMetadata(DynamicMessage.parseFrom(descriptor, message));
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException("Failed to parse protobuf message", e);
        }
    }

    /**
     * Builds gRPC metadata from a Protobuf message.
     *
     * @param message the Protobuf message
     * @return gRPC metadata
     */
    private Metadata buildGrpcMetadata(Message message) {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, String> entry : metadataTemplate.entrySet()) {
            String updatedKey = evaluateExpression(entry.getKey(), message).toString();
            Object updatedValue = evaluateExpression(entry.getValue(), message);
            metadata.put(Metadata.Key.of(updatedKey.trim(), Metadata.ASCII_STRING_MARSHALLER), updatedValue.toString());
        }
        return metadata;
    }

    /**
     * Evaluates a CEL expression or returns the input string if it's not a CEL expression.
     *
     * @param input   the expression to evaluate
     * @param message the Protobuf message used for evaluation
     * @return the evaluated result or the original expression if not a CEL expression
     */
    private Object evaluateExpression(String input, Message message) {
        Matcher matcher = CEL_EXPRESSION_MARKER.matcher(input);
        if (!matcher.find()) {
            return input;
        }
        String celExpression = matcher.group(EXACT_CEL_EXPRESSION_GROUP_INDEX);
        return Optional.ofNullable(celExpressionToProgramMap.get(celExpression))
                .map(program -> CelUtils.evaluate(program, message)).orElse(input);
    }

    /**
     * Initializes CEL programs for the metadata template.
     *
     * @return a map of CEL expressions to their corresponding programs
     */
    private Map<String, CelRuntime.Program> initializeCelPrograms() {
        CelRuntime celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
        CelCompiler celCompiler = CelUtils.initializeCelCompiler(this.descriptor);
        return this.metadataTemplate.entrySet()
                .stream()
                .filter(entry -> Objects.nonNull(entry.getValue()))
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                .map(string -> {
                    Matcher matcher = CEL_EXPRESSION_MARKER.matcher(string);
                    if (matcher.find()) {
                        return matcher.group(EXACT_CEL_EXPRESSION_GROUP_INDEX);
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toMap(Function.identity(), celExpression ->
                        CelUtils.initializeCelProgram(celExpression, celRuntime, celCompiler, celType -> celType.kind()
                                .isPrimitive())));
    }

}
