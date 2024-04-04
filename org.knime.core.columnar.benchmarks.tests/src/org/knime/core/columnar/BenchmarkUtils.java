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
 *   Dec 22, 2023 (benjamin): created
 */
package org.knime.core.columnar;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.knime.core.columnar.store.FileHandle;

/**
 * Utilities for Clumnar Backend benchmarks.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Berlin, Germany
 */
public final class BenchmarkUtils {

    private BenchmarkUtils() {
    }

    /** Options for missing values, all, some, or none */
    @SuppressWarnings("javadoc")
    public enum MissingValues {
            ALL_, SOME, NONE;
    }

    /**
     * Create a temporary {@link FileHandle}.
     *
     * @param prefix to help identifying which files belong to which benchmark
     * @return a file handle to a temporary file
     * @throws IOException if the file cannot be created
     */
    public static FileHandle createFileHandle(final String prefix) throws IOException {
        return new TestFileHandle(prefix);
    }

    private static final class TestFileHandle implements FileHandle {

        private final Path m_path;

        private TestFileHandle(final String prefix) throws IOException {
            m_path = Files.createTempFile(prefix, "table");
        }

        @Override
        public void delete() {
            try {
                Files.deleteIfExists(m_path);
            } catch (IOException ex) {
                throw new IllegalStateException("deleting tmp file failed", ex);
            }
        }

        @Override
        public Path asPath() {
            return m_path;
        }

        @Override
        public File asFile() {
            return m_path.toFile();
        }
    }
}
