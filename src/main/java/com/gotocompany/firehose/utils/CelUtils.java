package com.gotocompany.firehose.utils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelOptions;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.CelType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.extensions.CelExtensions;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import org.aeonbits.owner.util.Collections;

import java.util.function.Predicate;

/**
 * Utility class to instantiate CEL(Common Expression Language) related functionality.
 * <a href="https://github.com/google/cel-spec">Official Documentation of CEL</a>
 */
public class CelUtils {

    /**
     * @param program the program to execute
     * @param payload the proto payload to evaluated by program
     * @return the dynamic value based on program execution of the payload
     */
    public static Object evaluate(CelRuntime.Program program, Message payload) {
        try {
            return program.eval(Collections.map(payload.getDescriptorForType().getFullName(), payload));
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("Could not evaluate Cel expression", e);
        }
    }

    /**
     * Initializes the CEL compiler with standard macros and message types.
     *
     * @return the initialized CEL compiler
     */
    public static CelCompiler initializeCelCompiler(Descriptors.Descriptor descriptor) {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.values())
                .addVar(descriptor.getFullName(), StructTypeReference.create(descriptor.getFullName()))
                .addLibraries(CelExtensions.strings(), CelExtensions.bindings(), CelExtensions.math(CelOptions.DEFAULT), CelExtensions.protos())
                .addMessageTypes(descriptor)
                .build();
    }

    /**
     * Initializes the CEL runtime with standard libraries.
     *
     * @return the initialized CEL runtime
     */
    public static CelRuntime initializeCelRuntime() {
        return CelRuntimeFactory.standardCelRuntimeBuilder()
                .addLibraries(CelExtensions.strings(), CelExtensions.math(CelOptions.DEFAULT))
                .build();
    }

    /**
     * Initializes a CEL program for a given expression.
     *
     * @param celExpression     the CEL expression to compile
     * @param celRuntime        the CEL runtime environment
     * @param celCompiler       the CEL compiler
     * @param resultTypeChecker the predicate to evaluate whether return type is supported or not
     * @return the compiled CEL program
     * @throws IllegalArgumentException if the CEL program cannot be created
     * @throws UnsupportedOperationException if the return type is not supported
     */
    public static CelRuntime.Program initializeCelProgram(String celExpression,
                                                          CelRuntime celRuntime,
                                                          CelCompiler celCompiler,
                                                          Predicate<CelType> resultTypeChecker) {
        try {
            CelAbstractSyntaxTree celAbstractSyntaxTree = celCompiler.compile(celExpression)
                    .getAst();
            if (!resultTypeChecker.test(celAbstractSyntaxTree.getResultType())) {
                throw new UnsupportedOperationException("Return type not supported for " + celExpression);
            }
            return celRuntime.createProgram(celAbstractSyntaxTree);
        } catch (CelValidationException | CelEvaluationException e) {
            throw new IllegalArgumentException("Failed to create CEL program with expression : " + celExpression, e);
        }
    }

}
