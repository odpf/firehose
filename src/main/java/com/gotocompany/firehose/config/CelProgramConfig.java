package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

import java.util.List;

public interface CelProgramConfig extends Config {

    @Key("CEL_PROGRAM_PROTO_CLASSES")
    List<String> getCelProgramProtoClasses();

}
