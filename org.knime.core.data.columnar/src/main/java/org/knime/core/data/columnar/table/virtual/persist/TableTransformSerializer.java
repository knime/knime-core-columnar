/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 2, 2023 (Adrian Nembach, KNIME GmbH, Konstanz, Germany): created
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
        return loader.apply(transformSpecConfig.get("config"));
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
