'use client'

import type { CandidacySummary } from '@/types/api'
import DataTable, { type Column } from '@/components/ui/DataTable'
import ConfidenceBadge from '@/components/ui/ConfidenceBadge'
import NullState from '@/components/ui/NullState'
import { TABLE_HEADERS, RESULT_LABELS, NULL_LABELS, PROFILE } from '@/lib/constants'
import { formatVotes } from '@/lib/utils'

interface ElectoralTableProps {
  candidacies: CandidacySummary[]
  activeYear?: number | null
}

const columns: Column<CandidacySummary>[] = [
  {
    key: 'election_year',
    header: TABLE_HEADERS.year,
    sortable: true,
    render: (row) => (
      <span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--text-sm)' }}>
        {row.election_year}
      </span>
    ),
  },
  {
    key: 'office_name',
    header: TABLE_HEADERS.office,
    render: (row) =>
      row.office_name ? row.office_name : <NullState label={NULL_LABELS.office} />,
  },
  {
    key: 'party_abbr',
    header: TABLE_HEADERS.party,
    render: (row) =>
      row.party_abbr ? (
        <span style={{ fontFamily: 'var(--font-mono)' }}>{row.party_abbr}</span>
      ) : (
        <NullState label={NULL_LABELS.party} />
      ),
  },
  {
    key: 'territory',
    header: TABLE_HEADERS.territory,
    render: (row) =>
      row.territory ? row.territory : <NullState label={NULL_LABELS.territory} />,
  },
  {
    key: 'result',
    header: TABLE_HEADERS.result,
    render: (row) =>
      row.result ? (RESULT_LABELS[row.result] ?? row.result) : <NullState label={NULL_LABELS.result} />,
  },
  {
    key: 'vote_count',
    header: TABLE_HEADERS.votes,
    align: 'right',
    render: (row) =>
      row.vote_count != null ? (
        <span style={{ fontFamily: 'var(--font-mono)' }}>{formatVotes(row.vote_count)}</span>
      ) : (
        <NullState label="—" />
      ),
  },
  {
    key: 'confidence',
    header: TABLE_HEADERS.confidence,
    render: (row) => <ConfidenceBadge confidence={row.confidence} />,
  },
]

export default function ElectoralTable({ candidacies, activeYear }: ElectoralTableProps) {
  const sorted = [...candidacies].sort((a, b) => b.election_year - a.election_year)

  return (
    <div>
      <h2
        className="font-semibold mb-4"
        style={{
          fontFamily: 'var(--font-display)',
          color: 'var(--color-ink)',
          fontSize: 'var(--text-lg)',
        }}
      >
        {PROFILE.candidacies_title}
      </h2>
      <DataTable
        columns={columns}
        rows={sorted}
        rowKey={(row) => row.id}
        rowClassName={(row) =>
          activeYear != null && row.election_year === activeYear
            ? 'bg-white/[0.04]'
            : undefined
        }
      />
    </div>
  )
}
