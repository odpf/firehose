package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import io.grpc.Metadata;
import org.aeonbits.owner.util.Collections;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProtoToMetadataMapper {

    private static final Pattern CEL_EXPRESSION_MARKER = Pattern.compile("^\\$(.+)");
    private final Descriptors.Descriptor descriptor;
    private final CelRuntime celRuntime;
    private final CelCompiler celCompiler;
    private final Map<String, CelRuntime.Program> celExpressionToProgramMapper;
    private final Map<String, Object> metadataTemplate;

    public ProtoToMetadataMapper(Descriptors.Descriptor descriptor, Map<String, Object> metadataTemplate) {
        this.descriptor = descriptor;
        this.celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
        this.celCompiler = initializeCelCompiler();
        this.metadataTemplate = metadataTemplate;
        this.celExpressionToProgramMapper = initializeCelPrograms(metadataTemplate);
    }

    public Metadata buildGrpcMetadata(Message message) throws IOException {
        Metadata metadata = new Metadata();
        for (Map.Entry<String, Object> entry : metadataTemplate.entrySet()) {
            String updatedKey = evaluateValue(entry.getKey(), message).toString();
            Object updatedValue = entry.getValue() instanceof String ? evaluateValue(entry.getValue().toString(), message) : entry.getValue();
            metadata.put(Metadata.Key.of(updatedKey, Metadata.BINARY_BYTE_MARSHALLER), (byte[]) updatedValue);
        }
        return metadata;
    }

    private Object evaluateValue(String key, Message message) {
        Matcher matcher = CEL_EXPRESSION_MARKER.matcher(key);
        if (!matcher.find()) {
            return key;
        }
        return Optional.ofNullable(celExpressionToProgramMapper.get(matcher.group(1)))
                .map(program -> {
                    try {
                        Object val = program.eval(Collections.map(message.getDescriptorForType().getFullName(), message));
                        if (isComplexType(val)) {
                            throw new IllegalArgumentException("Complex type not supported");
                        }
                        return val;
                    } catch (CelEvaluationException e) {
                        throw new IllegalArgumentException("Could not evaluate " + key + ": " + e.getMessage());
                    }
                }).orElse(key);
    }

    private boolean isComplexType(Object object) {
        return !(object instanceof String || object instanceof Number || object instanceof Boolean);
    }

    private CelCompiler initializeCelCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.values())
                .addVar(this.descriptor.getFullName(), StructTypeReference.create(this.descriptor.getFullName()))
                .addMessageTypes(this.descriptor)
                .build();
    }

    private Map<String, CelRuntime.Program> initializeCelPrograms(Map<String, Object> metadataTemplate) {
        return metadataTemplate.entrySet()
                .stream()
                .filter(entry -> entry.getValue() instanceof String)
                .flatMap(e -> Stream.of(e.getKey(), e.getValue().toString()))
                .filter(keyword -> CEL_EXPRESSION_MARKER.matcher(keyword).matches())
                .map(keyword -> {
                    Matcher matcher = CEL_EXPRESSION_MARKER.matcher(keyword);
                    if (matcher.matches()) {
                        return matcher.group(1);
                    }
                    return StringUtils.EMPTY;
                })
                .filter(StringUtils::isNotBlank)
                .map(celExpression -> new AbstractMap.SimpleEntry<>(celExpression, initializeCelProgram(celExpression)))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private CelRuntime.Program initializeCelProgram(String celExpression) {
        try {
            CelAbstractSyntaxTree celAbstractSyntaxTree = celCompiler.compile(celExpression)
                    .getAst();
            return celRuntime.createProgram(celAbstractSyntaxTree);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException("Failed to create CEL program with expression : " + celExpression, e);
        }
    }

}
