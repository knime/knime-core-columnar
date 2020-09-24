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

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.knime.core.columnar.data.ColumnReadData;
import org.knime.core.columnar.data.ColumnDataSpec;
import org.knime.core.columnar.data.DoubleData;
import org.knime.core.columnar.data.IntData;
import org.knime.core.columnar.data.LongData;
import org.knime.core.columnar.data.StringData;
import org.knime.core.columnar.store.ColumnStoreSchema;
import org.knime.core.data.RowKeyConfig;
import org.knime.core.data.columnar.ColumnarRowWriteCursorConfig;
import org.knime.core.data.columnar.domain.DoubleDomain.DoubleDomainCalculator;
import org.knime.core.data.columnar.domain.IntDomain.IntDomainCalculator;
import org.knime.core.data.columnar.domain.LongDomain.LongDomainCalculator;
import org.knime.core.data.columnar.domain.StringDomain.StringDomainCalculator;
import org.knime.core.util.DuplicateChecker;

/**
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
public final class DefaultDomainStoreConfig implements DomainStoreConfig {

	private final ColumnStoreSchema m_spec;

	private final Map<Integer, ColumnarDomain> m_initialDomains;

	private final int m_maxPossibleNominalDomainValues;

	private final RowKeyConfig m_rowKeyConfig;

	public DefaultDomainStoreConfig(final ColumnStoreSchema spec, final Map<Integer, ColumnarDomain> initialDomains,
			final int maxPossibleNominalDomainValues, final RowKeyConfig rowKeyConfig) {
		m_spec = spec;
		m_initialDomains = initialDomains;
		m_maxPossibleNominalDomainValues = maxPossibleNominalDomainValues;
		m_rowKeyConfig = rowKeyConfig;
	}

	/**
	 * @param config {@link FastTableConfig#isInitializeDomains()
	 *               isInitializeDomains} is ignored and overridden by
	 *               {@code initialDomains}.
	 */
	public DefaultDomainStoreConfig(final ColumnStoreSchema schema, final Map<Integer, ColumnarDomain> initialDomains,
			final ColumnarRowWriteCursorConfig config) {
		this(schema, initialDomains, config.getMaxPossibleNominalDomainValues(), config.getRowKeyConfig());
	}

	@Override
	public DomainStoreConfig withMaxPossibleNominalDomainValues(final int maxPossibleValues) {
		return new DefaultDomainStoreConfig(m_spec, m_initialDomains, maxPossibleValues, m_rowKeyConfig);
	}

	@Override
	public DuplicateChecker createDuplicateChecker() {
		final DuplicateChecker duplicateChecker;
		switch (m_rowKeyConfig) {
		case CUSTOM:
			duplicateChecker = new DuplicateChecker();
			break;
		case NOKEY:
			duplicateChecker = null;
			break;
		default:
			throw new IllegalStateException("Unknown row key type: " + m_rowKeyConfig.name());
		}
		return duplicateChecker;
	}

	/*
	 * TODO: make sure we calculate the domains for all Nominal / Bounded values we
	 * support with the tables API. As soon as we support writing arbitrary
	 * DataCells we have to also calculate the domain for these Nominal / Bounded
	 * domains. Extension point?
	 */
	@Override
	public DomainCalculator<? extends ColumnReadData, ? extends ColumnarDomain>[] createCalculators() {
		final DomainCalculator<?, ?>[] domains = new DomainCalculator[m_spec.getNumColumns() - 1];
		for (int i = 1; i < m_spec.getNumColumns(); i++) {
			ColumnDataSpec spec = m_spec.getColumnDataSpec(i);
			final ColumnarDomain initialDomain = m_initialDomains.get(i);
			DomainCalculator<?, ?> calculator = null;
			if (spec instanceof DoubleData.DoubleDataSpec) {
				calculator = constructCalculator((DoubleDomain) initialDomain, DoubleDomainCalculator::new,
						DoubleDomainCalculator::new);
			} else if (spec instanceof IntData.IntDataSpec) {
				calculator = constructCalculator((IntDomain) initialDomain, IntDomainCalculator::new,
						IntDomainCalculator::new);
			} else if (spec instanceof LongData.LongDataSpec) {
				calculator = constructCalculator((LongDomain) initialDomain, LongDomainCalculator::new,
						LongDomainCalculator::new);
			} else if (spec instanceof StringData.StringDataSpec) {
				calculator = constructCalculator((StringDomain) initialDomain,
						d -> new StringDomainCalculator(m_maxPossibleNominalDomainValues, d),
						() -> new StringDomainCalculator(m_maxPossibleNominalDomainValues));
			}
			if (calculator != null) {
				domains[i - 1] = calculator;
			}
		}
		return domains;
	}

	private static <D extends ColumnarDomain> DomainCalculator<?, D> constructCalculator(final D initialDomain,
			final Function<D, ? extends DomainCalculator<?, D>> oneArgConstructor,
			final Supplier<? extends DomainCalculator<?, D>> noArgConstructor) {
		return initialDomain != null ? oneArgConstructor.apply(initialDomain) : noArgConstructor.get();
	}

}
