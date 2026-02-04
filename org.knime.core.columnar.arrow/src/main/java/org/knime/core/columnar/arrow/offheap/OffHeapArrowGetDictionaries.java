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
package org.knime.core.columnar.arrow.offheap;

import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.knime.core.columnar.data.NullableReadData;

/**
 * TODO javadoc or remove
 *
 * @author pietzsch
 */
public interface OffHeapArrowGetDictionaries {

    /**
     * Split out of OffHeapArrowColumnDataFactory.
     * <p>
     * Currently we do not write any data using Arrow dictionaries. Instead we use our own struct-based dictionary
     * encoding. The only reasons this method is here is so that we can write test data that uses Arrow dictionaries.
     *
     * @param data a column data holding some values
     * @return dictionaries which should be written to disk
     */
    DictionaryProvider getDictionaries(NullableReadData data);

    public static DictionaryProvider getDictionaries(final OffHeapArrowColumnDataFactory factory,
        final NullableReadData data) {

        if (factory instanceof OffHeapArrowGetDictionaries f) {
            return f.getDictionaries(data);
            //        } else if (factory instanceof ArrowStructDataFactory f) {
            //            final ArrowStructReadData d = (ArrowStructReadData)data;
            //            final List<DictionaryProvider> providers = new ArrayList<>();
            //            for (int i = 0; i < m_inner.length; i++) {
            //                final DictionaryProvider p = m_inner[i].getDictionaries(d.getReadDataAt(i));
            //                if (p != null) {
            //                    providers.add(p);
            //                }
            //            }
            //            return new NestedDictionaryProvider(providers);
            //        } else if (factory instanceof ArrowListDataFactory f) {
            //            final ArrowListReadData d = (ArrowListReadData)data;
            //            return m_inner.getDictionaries(d.m_data);

            // .... else if (factory instanceof ExtensionArrowColumnDataFactory.OffHeapArrowSchemaMapper) ...
        }
        return null;
    }

}
