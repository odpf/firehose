package com.gotocompany.firehose.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.firehose.exception.OperationNotSupportedException;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.aeonbits.owner.util.Collections;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
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
    private final String metadataTemplate;

    public ProtoToMetadataMapper(Descriptors.Descriptor descriptor, String metadataTemplate) {
        this.descriptor = descriptor;
        this.celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder().build();
        this.celCompiler = initializeCelCompiler();
        this.metadataTemplate = metadataTemplate;
        this.celExpressionToProgramMapper = initializeCelPrograms(metadataTemplate);
    }

    public Map<String, Object> buildGrpcMetadata(Message message) throws IOException {
        Map<String, Object> parsedCelExpressions = parseCelExpressions(message);
        String populatedHeaderTemplate = parsedCelExpressions.entrySet()
                .stream()
                .reduce(metadataTemplate,
                        (template, entry) -> template.replace(String.format("$%s", entry.getKey()), entry.getValue().toString()),
                        (previousTemplate, currentTemplate) -> currentTemplate);
        return new ObjectMapper().readValue(populatedHeaderTemplate, new TypeReference<Map<String, Object>>() {
        });
    }

    private Map<String, Object> parseCelExpressions(Message message) {
        if (!descriptor.getFullName().equals(message.getDescriptorForType().getFullName())) {
            throw new IllegalArgumentException("Payload not of type " + descriptor.getFullName());
        }
        return celExpressionToProgramMapper.entrySet()
                .stream()
                .map(entry -> parseSinglePayload(message, entry))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    private AbstractMap.SimpleEntry<String, Object> parseSinglePayload(Message message, Map.Entry<String, CelRuntime.Program> entry) {
        try {
            Object parsedValue = entry.getValue().eval(Collections.map(message.getDescriptorForType().getFullName(), message));
            if (isComplexType(parsedValue)) {
                throw new OperationNotSupportedException("Complex type not supported");
            }
            return new AbstractMap.SimpleEntry<>(entry.getKey(), parsedValue);
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("Cannot evaluate expression: " + entry.getKey(), e);
        }
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

    private Map<String, CelRuntime.Program> initializeCelPrograms(String metadataTemplate) {
        try {
            Map<String, String> parsedMetadataTemplate = new ObjectMapper().readValue(metadataTemplate, new TypeReference<Map<String, String>>() {
            });
            return parsedMetadataTemplate.entrySet()
                    .stream()
                    .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()))
                    .filter(string -> CEL_EXPRESSION_MARKER.matcher(string).matches())
                    .map(string -> {
                        Matcher matcher = CEL_EXPRESSION_MARKER.matcher(string);
                        if (matcher.find()) {
                            return matcher.group(1);
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .map(celExpression -> new AbstractMap.SimpleEntry<>(celExpression, initializeCelProgram(celExpression)))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed parsing JSON metadata template: " + metadataTemplate, e);
        }
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
