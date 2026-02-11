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
 *   27 Jan 2026 (pietzsch): created
 */
package org.knime.core.columnar.arrow.offheap.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.arrow.offheap.OffHeapArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.OffHeapLegacyDictionaryArrowColumnDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowListData.ArrowListDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowListData.ArrowListReadData;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStructData.ArrowStructDataFactory;
import org.knime.core.columnar.arrow.offheap.data.OffHeapArrowStructData.ArrowStructReadData;
import org.knime.core.columnar.data.NullableReadData;

/**
 * Currently we do not write any data using Arrow dictionaries. Instead we use our own struct-based dictionary encoding.
 * However, we still want to write test data that uses Arrow dictionaries. For this purpose test
 * {@code OffHeapTestArrowColumnDataFactory} can create Arrow dictionaries and provide access to the dictionaries used
 * the resulting {@code NullableReadData}.
 * <p>
 * This class allows to extract Arrow dictionaries from {@code NullableReadData} that may use them (directly or in
 * nested data).
 *
 * @author Tobias Pietzsch
 */
public final class OffHeapArrowLegacyDictionaryUtils {

    private OffHeapArrowLegacyDictionaryUtils() {
        // utility class, don't instantiate
    }

    /**
     * Extract Arrow dictionaries used in the given {@code data} (as well as nested data).
     *
     * @param factory factory that produced the data
     * @param data the data
     * @return dictionaries used in data
     */
    public static DictionaryProvider getDictionaries(final OffHeapArrowColumnDataFactory factory,
        final NullableReadData data) {

        if (factory instanceof OffHeapLegacyDictionaryArrowColumnDataFactory f) {
            return f.getDictionaries(data);
        } else if (factory instanceof ArrowStructDataFactory f) {
            final ArrowStructReadData d = (ArrowStructReadData)data;
            final List<DictionaryProvider> providers = new ArrayList<>();
            for (int i = 0; i < f.m_inner.length; i++) {
                final DictionaryProvider p = getDictionaries(f.m_inner[i], d.getReadDataAt(i));
                if (p != null) {
                    providers.add(p);
                }
            }
            return providers.isEmpty() ? null : new NestedDictionaryProvider(providers);
        } else if (factory instanceof ArrowListDataFactory f) {
            final ArrowListReadData d = (ArrowListReadData)data;
            return getDictionaries(f.m_inner, d.m_data);
        } else {
            return null;
        }
    }

    /**
     * Check whether any of the {@code factories} use Arrow dictionaries.
     *
     * @param factories factories to check
     * @return {@code true} if any of the factories uses Arrow dictionaries
     */
    public static boolean usesDictionaries(final OffHeapArrowColumnDataFactory[] factories) {
        for (int i = 0; i < factories.length; i++) {
            if (usesDictionaries(factories[i])) {
                return true;
            }
        }
        return false;
    }

    private static boolean usesDictionaries(final OffHeapArrowColumnDataFactory factory) {
        if (factory instanceof ArrowStructDataFactory f) {
            return usesDictionaries(f.m_inner);
        } else if (factory instanceof ArrowListDataFactory f) {
            return usesDictionaries(f.m_inner);
        } else {
            return (factory instanceof OffHeapLegacyDictionaryArrowColumnDataFactory);
        }
    }

    /**
     * A dictionary provider holding a list of dictionary providers. On {@link #lookup(long)} the lookup is performed on
     * the children dictionary providers until the id is found.
     */
    public static final class NestedDictionaryProvider implements DictionaryProvider {

        private final List<DictionaryProvider> m_providers;

        /**
         * Create a dictionary provider with the given children
         *
         * @param providers the providers to lookup the ids
         */
        public NestedDictionaryProvider(final List<DictionaryProvider> providers) {
            m_providers = providers;
        }

        @Override
        public Dictionary lookup(final long id) {
            for (int i = 0; i < m_providers.size(); i++) {
                final Dictionary dictionary = m_providers.get(i).lookup(id);
                if (dictionary != null) {
                    return dictionary;
                }
            }
            return null;
        }

        @Override
        public Set<Long> getDictionaryIds() {
            return m_providers.stream() //
                .flatMap(p -> p.getDictionaryIds().stream()) //
                .collect(Collectors.toSet());
        }
    }
}
