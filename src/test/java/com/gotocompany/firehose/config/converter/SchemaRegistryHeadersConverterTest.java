package com.gotocompany.firehose.config.converter;

import java.util.HashMap;
import java.util.Map;

import com.gotocompany.firehose.config.AppConfig;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.message.BasicHeader;
import org.junit.Assert;
import org.junit.Test;

public class SchemaRegistryHeadersConverterTest {
  @Test
  public void testConvertIfFetchHeadersValueEmpty() {
    Map<String, String> properties = new HashMap<String, String>() {
      {
        put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "");
      }
    };
    AppConfig config = ConfigFactory.create(AppConfig.class, properties);
    Assert.assertEquals(0, config.getSchemaRegistryFetchHeaders().size());
  }

  @Test
  public void shouldReturnZeroIfPropertyNotMentioned() {
    Map<String, String> properties = new HashMap<String, String>() {
    };
    AppConfig config = ConfigFactory.create(AppConfig.class, properties);
    Assert.assertEquals(0, config.getSchemaRegistryFetchHeaders().size());
  }

  @Test
  public void shouldConvertHeaderKeyValuesWithHeaderObject() {
    Map<String, String> properties = new HashMap<String, String>() {
      {
        put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "key1:value1 ,,, key2 : value2,");
      }
    };
    AppConfig config = ConfigFactory.create(AppConfig.class, properties);
    Assert.assertEquals((new BasicHeader("key1", "value1")).toString(), config.getSchemaRegistryFetchHeaders().get(0).toString());
    Assert.assertEquals((new BasicHeader("key2", "value2")).toString(), config.getSchemaRegistryFetchHeaders().get(1).toString());
    Assert.assertEquals(2, config.getSchemaRegistryFetchHeaders().size());
  }
}
