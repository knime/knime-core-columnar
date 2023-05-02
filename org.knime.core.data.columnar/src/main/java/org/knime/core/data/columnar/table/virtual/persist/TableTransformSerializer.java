/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on May 20, 2021 by marcel
 */
package org.knime.core.data.columnar.table.virtual.persist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.knime.core.table.virtual.TableTransform;
import org.knime.core.table.virtual.spec.AppendTransformSpec;
import org.knime.core.table.virtual.spec.ConcatenateTransformSpec;
import org.knime.core.table.virtual.spec.IdentityTransformSpec;
import org.knime.core.table.virtual.spec.SelectColumnsTransformSpec;
import org.knime.core.table.virtual.spec.SliceTransformSpec;
import org.knime.core.table.virtual.spec.SourceTransformSpec;
import org.knime.core.table.virtual.spec.TableTransformSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * @author Marcel Wiedenmann, KNIME GmbH, Konstanz, Germany
 * @author Adrian Nembach, KNIME GmbH, Konstanz, Germany
 * @deprecated use {@link TableTransformNodeSettingsPersistor} instead
 */
@Deprecated
public final class TableTransformSerializer {

    private TableTransformSerializer() {}

    public static TableTransform load(final JsonNode config) {
        return new TableTransformInDeserialization(config).load();
    }

    public static TableTransformSpec deserializeTransformSpec(final JsonNode transformSpecConfig) {
        final String type = transformSpecConfig.get("type").textValue();
        var loader = getLoader(type);
        return loader.apply(transformSpecConfig);
    }

    private static Function<JsonNode, TableTransformSpec> getLoader(final String transformSpecType) {//NOSONAR
        switch (transformSpecType) {
            case "append":
                return c -> new AppendTransformSpec();
            case "permute":
                return TableTransformSerializer::loadPermuteBackwardsCompatible;
            case "column_filter", "select_columns":
                return TableTransformSerializer::loadSelect;
            case "concatenate":
                return c -> new ConcatenateTransformSpec();
            case "slice":
                return c -> new SliceTransformSpec(c.get("from").longValue(), c.get("to").longValue());
            case "source":
                return c -> new SourceTransformSpec(UUID.fromString(c.get("identifier").textValue()), null);
            case "identity":
                return c -> IdentityTransformSpec.INSTANCE;
            default:
                throw new UnsupportedOperationException("Unkown transformation: " + transformSpecType);
        }
    }

    private static SelectColumnsTransformSpec loadSelect(final JsonNode input) {
        final ObjectNode root = (ObjectNode)input;
        final ArrayNode columnIndicesConfig = (ArrayNode)root.get("included_columns");
        final int[] columnIndices = new int[columnIndicesConfig.size()];
        for (int i = 0; i < columnIndices.length; i++) {
            columnIndices[i] = columnIndicesConfig.get(i).intValue();
        }
        return new SelectColumnsTransformSpec(columnIndices);
    }


    private static final class TableTransformInDeserialization {

        private final ObjectNode m_configRoot;

        public TableTransformInDeserialization(final JsonNode input) {
            m_configRoot = (ObjectNode)input;
        }

        public TableTransform load() {
            final ArrayNode transformsConfig = (ArrayNode)m_configRoot.get("transforms");
            final ArrayNode connectionsConfig = (ArrayNode)m_configRoot.get("connections");

            final List<TableTransformSpec> transformSpecs = new ArrayList<>(transformsConfig.size());
            for (final JsonNode transformConfig : transformsConfig) {
                transformSpecs.add(deserializeTransformSpec(transformConfig));
            }

            final Set<Integer> leafTransforms =
                new LinkedHashSet<>(IntStream.range(0, transformSpecs.size()).boxed().collect(Collectors.toList()));
            final Map<Integer, Map<Integer, Integer>> parentTransforms = new HashMap<>();
            for (final JsonNode connection : connectionsConfig) {
                final int fromTransform = connection.get("from").get("transform").intValue();
                leafTransforms.remove(fromTransform);
                final JsonNode to = connection.get("to");
                final int toTransform = to.get("transform").intValue();
                final int toPort = to.get("port").intValue();
                parentTransforms.computeIfAbsent(toTransform, k -> new HashMap<>()).put(toPort, fromTransform);
            }

            final Map<Integer, TableTransform> transforms = new HashMap<>();
            for (int i = 0; i < transformSpecs.size(); i++) {
                resolveTransformsTree(i, transformSpecs, parentTransforms, transforms);
            }

            // TODO: support returning multi-output graphs
            return transforms.get(leafTransforms.iterator().next());
        }

        // TODO: our serialization logic above guarantees a topological ordering of the serialized graph representation.
        // This should allow us to simplify this method (i.e. getting rid of the recursion).
        private static void resolveTransformsTree(final int specIndex, final List<TableTransformSpec> transformSpecs,
            final Map<Integer, Map<Integer, Integer>> parentTransforms, final Map<Integer, TableTransform> transforms) {
            if (transforms.containsKey(specIndex)) {
                return;
            }
            final Map<Integer, Integer> parents = parentTransforms.get(specIndex);
            final List<TableTransform> resolvedParents;
            if (parents != null) {
                resolvedParents = new ArrayList<>(parents.size());
                for (int j = 0; j < parents.size(); j++) {
                    final int parentSpecIndex = parents.get(j);
                    resolveTransformsTree(parentSpecIndex, transformSpecs, parentTransforms, transforms);
                    resolvedParents.add(transforms.get(parentSpecIndex));
                }
            } else {
                resolvedParents = Collections.emptyList();
            }
            transforms.put(specIndex, new TableTransform(resolvedParents, transformSpecs.get(specIndex)));
        }

    }

    private static SelectColumnsTransformSpec loadPermuteBackwardsCompatible(final JsonNode input) {
        final ObjectNode config = (ObjectNode)input;
        final ArrayNode permutationConfig = (ArrayNode)config.get("permutation");
        final var permutation = new int[permutationConfig.size()];
        for (int i = 0; i < permutation.length; i++) {//NOSONAR
            permutation[i] = permutationConfig.get(i).intValue();
        }
        return new SelectColumnsTransformSpec(permutation);
    }
}
