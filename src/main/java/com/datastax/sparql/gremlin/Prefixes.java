/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datastax.sparql.gremlin;

import org.apache.xerces.util.URI;

import java.util.Arrays;
import java.util.List;


public class Prefixes {

    private final static String BASE_URI_SCHEME = "http";
    private final static String BASE_URI_HOST = "northwind.com";

	public final static String BASE_URI = String.format("%s://%s/model/", BASE_URI_SCHEME, BASE_URI_HOST);
   // public final static String BASE_URI = "http://www.tinkerpop.com/traversal/";

    final static List<String> PREFIXES = Arrays.asList("edge", "property", "value", "edge-proposition", "edge-proposition-subject", "vertex-id");
    final static List<String> SHORT_PREFIXES = Arrays.asList("e", "p", "v", "ep", "eps", "vid");

    final static String PREFIX_DEFINITIONS;

    static {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < PREFIXES.size(); ++i) {
            final String prefix = PREFIXES.get(i);
            final String shortPrefix = SHORT_PREFIXES.get(i);
            builder.append("PREFIX ").append(shortPrefix).append(": <").append(getURI(prefix)).
                    append(">").append(System.lineSeparator());
        }
        PREFIX_DEFINITIONS = builder.toString();
    }

    public static String getURI(final String prefix) {
        return BASE_URI + prefix + "#";
    }

    public static String getURIValue(final String uri) {
        return uri.substring(uri.indexOf("#") + 1);
    }

    public static String getPrefix(final String uri) {
        final String tmp = uri.substring(0, uri.indexOf("#"));
        return tmp.substring(tmp.lastIndexOf("/") + 1);
    }

    public static String prepend(final String script) {
        return PREFIX_DEFINITIONS + script;
    }

    public static StringBuilder prepend(final StringBuilder scriptBuilder) {
        return scriptBuilder.insert(0, PREFIX_DEFINITIONS);
    }

    public static boolean isValidVertexIdUri(String uriStr) {
        return isValidIdUri(uriStr, "/model/vertex-id");
    }

    public static boolean isValidEdgeIdUri(String uriStr) {
        return isValidIdUri(uriStr, "/model/edge");
    }

    public static boolean isValidPropertyIdUri(String uriStr) {
        return isValidIdUri(uriStr, "/model/property");
    }

    private static boolean isValidIdUri(String uriStr, String uriPath) {
        try {
            URI objectURI = new URI(uriStr);
            String objectPrefixHost = objectURI.getHost();
            String objectPrefixPath = objectURI.getPath();

            return
                objectPrefixHost.equalsIgnoreCase(BASE_URI_HOST) &&
                objectPrefixPath.equalsIgnoreCase(uriPath);
        } catch (Exception e) {
            return false;
        }
    }

    public static String createVertexIdUri(String vertexId) {
        return BASE_URI + "vertex-id#" + vertexId;
    }

    public static String createEdgeIdUri(String edgeId) {
        return BASE_URI + "edge#" + edgeId;
    }

    public static String createPropertyIdUri(String propertyId) {
        return BASE_URI + "property#" + propertyId;
    }
}
