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
 *   Sep 24, 2020 (benjamin): created
 */
package org.knime.core.columnar.arrow;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * Utility class for Arrow.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class ArrowReaderWriterUtils {

    private ArrowReaderWriterUtils() {
        // Class holding constants
    }

    /** The Arrow magic number. These are the first and last bytes of each arrow file */
    static final byte[] ARROW_MAGIC_BYTES = "ARROW1".getBytes(StandardCharsets.UTF_8);

    /** The length of the Arrow magic number */
    static final int ARROW_MAGIC_LENGTH = ARROW_MAGIC_BYTES.length;

    /** Key for the metadata element holding the chunk size */
    static final String ARROW_CHUNK_SIZE_KEY = "KNIME:basic:chunkSize";

    /** Key for the metadata element holding the factory versions */
    static final String ARROW_FACTORY_VERSIONS_KEY = "KNIME:basic:factoryVersions";

    /** A dictionary provider only holding one single dictionary */
    public static final class SingletonDictionaryProvider implements DictionaryProvider {

        private long m_id;

        private final Dictionary m_dictionary;

        /**
         * Create a dictionary provider only holding the given dictionary.
         *
         * @param dictionary the dictionary
         */
        public SingletonDictionaryProvider(final Dictionary dictionary) {
            m_dictionary = dictionary;
            m_id = dictionary.getEncoding().getId();
        }

        @Override
        public Dictionary lookup(final long id) {
            if (id == m_id) {
                return m_dictionary;
            } else {
                return null;
            }
        }
    }
}
