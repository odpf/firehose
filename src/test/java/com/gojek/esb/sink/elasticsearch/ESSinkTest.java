package com.gojek.esb.sink.elasticsearch;

import com.gojek.esb.config.ESSinkConfig;
import com.gojek.esb.consumer.EsbMessage;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.metrics.StatsDReporter;
import com.gojek.esb.sink.elasticsearch.client.ESSinkClient;
import org.aeonbits.owner.ConfigFactory;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class ESSinkTest {

    private ESSink esSink;
    private ESRequestBuilder esRequestBuilder;
    private ESSinkClient esSinkClient;
    private ESSinkConfig esSinkConfig;
    private Map<String, String> configuration;
    private String index;
    private String type;

    @Mock
    private StatsDReporter client;

    private String elasticsearchServer;

    @Before
    public void setUp() {
        elasticsearchServer = System.getenv("ELASTICSEARCH_SERVER");
        configuration = new HashMap<>();
        configuration.put("ES_BATCH_RETRY_COUNT", "3");
        configuration.put("ES_BATCH_SIZE", "1000");
        configuration.put("ES_CONNECTION_URLS", elasticsearchServer + ": 9200 , " + elasticsearchServer + " : 9200 ");

        esSinkConfig = ConfigFactory.create(ESSinkConfig.class, configuration);
        esSinkClient = new ESSinkClient(esSinkConfig, client);
        esRequestBuilder = new ESRequestBuilder(ESRequestType.INSERT_OR_UPDATE);
        index = "i-customer-tagstore";
        type = "customer";
        esSink = new ESSink(esRequestBuilder, esSinkClient, type, index);
    }

    @Test
    public void shouldPushMessageToES() throws IOException {
        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage1 = new EsbMessage(null, jsonString.getBytes(), "", 0, 1);
        String jsonString2 = "{\"customer_id\":\"542545041\",\"categories\":[{\"category\":\"ANEKA_AYAM_BEBEK\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"BAKMIE\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542736980\",\"merchant_uuid\":\"6e06be57-803c-4268-8423-acd4c4a39918\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"4167\",\"merchant_uuid\":\"0c8de88a-5462-42f6-b0bc-265dc16313b2\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":6}],\"brands\":[{\"brand_id\":\"d3f16a3f-8b4b-462f-90e4-250d125a152f\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"abcc4502-e823-40e7-a8c1-8da9c379880d\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"merchant_visits_4_weeks\":3,\"active_vouchers_count\":1.0,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"1\",\"current_country\":\"ID\",\"os\":\"iOS\",\"wallet_id\":\"1623809511595191926\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage2 = new EsbMessage(null, jsonString2.getBytes(), "", 0, 1);

        List<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);
        try {
            esSink.pushMessage(esbMessages);
        } catch (DeserializerException e) {
            e.printStackTrace();
        } finally {
            esSink.close();
        }

        Assert.assertTrue(assertThatMessagesWerePushed(esbMessages, index, type));
    }

    @Test
    public void shouldNotPushMessageToESDueToTypeMismatch() throws IOException {
        esSink = new ESSink(esRequestBuilder, esSinkClient, "wrong-type", index);
        String jsonString = "{\"customer_id\":\"544131695\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage1 = new EsbMessage(null, jsonString.getBytes(), "", 0, 1);
        String jsonString2 = "{\"customer_id\":\"542545096\",\"categories\":[{\"category\":\"ANEKA_AYAM_BEBEK\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"BAKMIE\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542736980\",\"merchant_uuid\":\"6e06be57-803c-4268-8423-acd4c4a39918\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"4167\",\"merchant_uuid\":\"0c8de88a-5462-42f6-b0bc-265dc16313b2\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":6}],\"brands\":[{\"brand_id\":\"d3f16a3f-8b4b-462f-90e4-250d125a152f\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"abcc4502-e823-40e7-a8c1-8da9c379880d\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"merchant_visits_4_weeks\":3,\"active_vouchers_count\":1.0,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"1\",\"current_country\":\"ID\",\"os\":\"iOS\",\"wallet_id\":\"1623809511595191926\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage2 = new EsbMessage(null, jsonString2.getBytes(), "", 0, 1);

        List<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);
        try {
            esSink.pushMessage(esbMessages);
        } catch (DeserializerException e) {
            e.printStackTrace();
        } finally {
            esSink.close();
        }

        Assert.assertFalse(assertThatMessagesWerePushed(esbMessages, index, type));
    }

    @Test
    public void shouldPushMessageToESByUpdateOnlyMode() throws IOException {
        esRequestBuilder = new ESRequestBuilder(ESRequestType.UPDATE_ONLY);
        esSink = new ESSink(esRequestBuilder, esSinkClient, type, index);

        String jsonString = "{\"customer_id\":\"544131618\",\"categories\":[{\"category\":\"COFFEE_SHOP\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"PIZZA_PASTA\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"ROTI\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"FASTFOOD\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542629489\",\"merchant_uuid\":\"62598e60-1e5b-497c-b971-5a2bb0efb745\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542777412\",\"merchant_uuid\":\"0a84a08b-8a53-47f4-9e62-7b7c2316dd08\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542675785\",\"merchant_uuid\":\"daf41597-27d4-4475-b7c7-4f11563adcdb\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1},{\"merchant_id\":\"542704646\",\"merchant_uuid\":\"9b522ca0-3ff0-4591-b60b-0e84b48d6d12\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"542809106\",\"merchant_uuid\":\"b902f7ba-ab5e-4de1-9755-56648f556265\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":1}],\"brands\":[{\"brand_id\":\"e9f7c4b2-4fa6-489a-ab20-a1bb4638ad29\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"336eb59c-621a-4704-811c-e1024f970e2e\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"0f30e2ca-f97f-43ec-895c-0d9d729e4cca\",\"merchant_visits_4_weeks\":0,\"orders_4_weeks\":1,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"901af18e-f5b7-43c5-9e67-4906d6ccce51\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"da07057d-7fe1-47de-8713-4c1edcfc9afc\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":2,\"orders_24_weeks\":2,\"merchant_visits_4_weeks\":4,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"2\",\"current_country\":\"ID\",\"os\":\"Android\",\"wallet_id\":\"16230097256391350739\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage1 = new EsbMessage(null, jsonString.getBytes(), "", 0, 1);
        String jsonString2 = "{\"customer_id\":\"542545041\",\"categories\":[{\"category\":\"ANEKA_AYAM_BEBEK\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"category\":\"BAKMIE\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"merchants\":[{\"merchant_id\":\"542736980\",\"merchant_uuid\":\"6e06be57-803c-4268-8423-acd4c4a39918\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":2000},{\"merchant_id\":\"4167\",\"merchant_uuid\":\"0c8de88a-5462-42f6-b0bc-265dc16313b2\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0,\"days_since_last_order\":6}],\"brands\":[{\"brand_id\":\"d3f16a3f-8b4b-462f-90e4-250d125a152f\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":0,\"allocated\":0.0,\"redeemed\":0.0},{\"brand_id\":\"abcc4502-e823-40e7-a8c1-8da9c379880d\",\"merchant_visits_4_weeks\":1,\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"allocated\":0.0,\"redeemed\":0.0}],\"orders_4_weeks\":0,\"orders_24_weeks\":1,\"merchant_visits_4_weeks\":3,\"active_vouchers_count\":1.0,\"app_version_major\":\"3\",\"app_version_minor\":\"30\",\"app_version_patch\":\"1\",\"current_country\":\"ID\",\"os\":\"iOS\",\"wallet_id\":\"1623809511595191926\",\"dag_run_time\":\"2019-06-27T07:27:00+00:00\"}";
        EsbMessage esbMessage2 = new EsbMessage(null, jsonString2.getBytes(), "", 0, 1);

        List<EsbMessage> esbMessages = new ArrayList<>();
        esbMessages.add(esbMessage1);
        esbMessages.add(esbMessage2);

        try {
            esSink.pushMessage(esbMessages);
        } catch (DeserializerException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(assertThatMessagesWerePushed(esbMessages, index, type));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgsExceptionForEmptyESUrls() {
        configuration = new HashMap<>();
        configuration.put("ES_BATCH_RETRY_COUNT", "3");
        configuration.put("ES_BATCH_SIZE", "1000");

        esSinkConfig = ConfigFactory.create(ESSinkConfig.class, configuration);
        esSinkClient = new ESSinkClient(esSinkConfig, client);
        Assert.fail("Should not have reached here");
    }

    private boolean assertThatMessagesWerePushed(List<EsbMessage> esbMessages, String indexName, String typeName) throws IOException {
        boolean allMessagesPushed = false;
        for (EsbMessage esbMessage : esbMessages) {
            String id = esRequestBuilder.extractId(esbMessage);
            GetResponse documentFields = esSinkClient.getRestHighLevelClient().get(new GetRequest(indexName, typeName, id));
            allMessagesPushed = documentFields.isExists();
        }
        return allMessagesPushed;
    }
}
