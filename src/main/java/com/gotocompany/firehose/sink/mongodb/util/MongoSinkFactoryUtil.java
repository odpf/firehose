package com.gotocompany.firehose.sink.mongodb.util;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.mongodb.ServerAddress;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The utility class for assisting in the creation and
 * configuration of a MongoSinkClient.
 *
 * @since 0.1
 */
@UtilityClass
public class MongoSinkFactoryUtil {

    /**
     * Extracts the MongoDB Server URLs from the connection URLs string and converts the
     * URL of each MongoDB server to a ServerAddress object and then stores these addresses
     * of all the MongoDB servers into a list, which is returned.
     *
     * @param mongoConnectionUrls the mongo connection urls
     * @param firehoseInstrumentation     the instrumentation
     * @return the list of server addresses
     * @throws IllegalArgumentException if the environment variable SINK_MONGO_CONNECTION_URLS
     *                                  is an empty string or not assigned any value by the user.
     *                                  This exception is also thrown if the URL does not contain
     *                                  any or both of hostname/ IP address and the port
     * @since 0.1
     */
    public static List<ServerAddress> getServerAddresses(String mongoConnectionUrls, FirehoseInstrumentation firehoseInstrumentation) {
        if (mongoConnectionUrls != null && !mongoConnectionUrls.isEmpty()) {
            List<String> mongoNodes = Arrays.asList(mongoConnectionUrls.trim().split(","));
            List<ServerAddress> serverAddresses = new ArrayList<>(mongoNodes.size());

            mongoNodes.forEach((String mongoNode) -> {
                List<String> node = Arrays.stream(mongoNode.trim().split(":"))
                        .filter(nodeString -> !nodeString.isEmpty()).collect(Collectors.toList());
                if (node.size() != 2) {
                    throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS should contain host and port both");
                }
                serverAddresses.add(new ServerAddress(node.get(0).trim(), Integer.parseInt(node.get(1).trim())));
            });
            return serverAddresses;
        } else {
            firehoseInstrumentation.logError("No connection URL found");
            throw new IllegalArgumentException("SINK_MONGO_CONNECTION_URLS is empty or null");
        }
    }
}
