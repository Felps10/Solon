import Link from 'next/link'
import { getPerson } from '@/lib/api'
import IdentityHeader from '@/components/profile/IdentityHeader'
import TimelineRail from '@/components/profile/TimelineRail'
import ElectoralTable from '@/components/profile/ElectoralTable'
import SnapshotWidget from '@/components/profile/SnapshotWidget'
import { PROFILE } from '@/lib/constants'

interface PageProps {
  params: Promise<{ id: string }>
  searchParams: Promise<{ as_of?: string }>
}

export default async function PessoaPage({ params, searchParams }: PageProps) {
  const { id } = await params
  const { as_of: asOf } = await searchParams

  // Derive activeYear from as_of param (e.g. "2018-12-31" → 2018)
  const activeYear = asOf ? parseInt(asOf.slice(0, 4), 10) : null

  let person = null
  let notFound = false

  try {
    person = await getPerson(id, asOf)
  } catch {
    notFound = true
  }

  if (notFound || !person) {
    return (
      <div className="max-w-5xl mx-auto px-6 py-24 text-center">
        <p
          className="text-4xl mb-4"
          style={{ color: 'var(--color-muted)', fontFamily: 'var(--font-display)' }}
        >
          Não encontrado
        </p>
        <p className="text-sm" style={{ color: 'var(--color-muted)' }}>
          Nenhum registro encontrado para o identificador{' '}
          <code style={{ fontFamily: 'var(--font-mono)' }}>{id}</code>.
        </p>
        <Link
          href="/"
          className="mt-6 inline-block text-sm"
          style={{ color: 'var(--color-accent)' }}
        >
          {PROFILE.back_to_search}
        </Link>
      </div>
    )
  }

  return (
    <div className="max-w-5xl mx-auto px-6 py-10">
      <Link
        href="/"
        className="inline-block mb-6 text-sm"
        style={{ color: 'var(--color-muted)' }}
      >
        {PROFILE.back_to_search}
      </Link>

      <IdentityHeader person={person} candidacies={person.candidacies} />

      <div className="mt-8 flex flex-col lg:flex-row gap-10">
        <div className="flex-1 min-w-0">
          <TimelineRail
            candidacies={person.candidacies}
            activeYear={activeYear}
            personId={id}
          />
          <ElectoralTable candidacies={person.candidacies} activeYear={activeYear} />
        </div>
        <aside className="w-full lg:w-64 flex-shrink-0">
          <SnapshotWidget personId={id} snapshotDate={person.snapshot_date} />
        </aside>
      </div>
    </div>
  )
}
