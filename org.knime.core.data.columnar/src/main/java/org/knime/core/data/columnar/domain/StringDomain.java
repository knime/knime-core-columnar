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
 */

package org.knime.core.data.columnar.domain;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.knime.core.columnar.data.StringData.StringReadData;

public final class StringDomain implements NominalDomain<String> {

	private static final StringDomain EMPTY = new StringDomain();

	private final Set<String> m_values;

	private StringDomain() {
		m_values = Collections.emptySet();
	}

	public StringDomain(final Set<String> values) {
		m_values = values != null ? Collections.unmodifiableSet(values) : null;
	}

	@Override
	public boolean isValid() {
		return m_values != null;
	}

	@Override
	public Set<String> getValues() {
		return m_values;
	}

	public static final class StringDomainCalculator extends AbstractDomainCalculator<StringReadData, StringDomain> {

		private final int m_numMaxValues;

		public StringDomainCalculator(final int numMaxValues) {
			this(numMaxValues, EMPTY);
		}

		public StringDomainCalculator(final int numMaxValues, final StringDomain initialDomain) {
			super(initialDomain);
			m_numMaxValues = numMaxValues;
		}

		@Override
		public StringDomain apply(final StringReadData data) {
			// TODO: more efficient implementation?
			// Preserve order
			Set<String> values = new LinkedHashSet<>();
			for (int i = 0; i < data.length(); i++) {
				if (!data.isMissing(i)) {
					values.add(data.getString(i));
					if (values.size() > m_numMaxValues) {
						// Null indicates that domain could not be computed due to excessive
						// distinct elements. Computed domain will be marked invalid.
						values = null;
						break;
					}
				}
			}
			return new StringDomain(values);
		}

		@Override
		public StringDomain merge(final StringDomain original, final StringDomain additional) {
			Set<String> union;
			if (original.isValid() && additional.isValid()) {
				// Preserve order
				union = new LinkedHashSet<>(original.getValues());
				union.addAll(additional.getValues());
				if (union.size() > m_numMaxValues) {
					// Null indicates that domain could not be computed due to excessive
					// distinct elements. Merged domain will be marked invalid.
					union = null;
				}
			} else {
				union = null;
			}
			return new StringDomain(union);
		}
	}
}
