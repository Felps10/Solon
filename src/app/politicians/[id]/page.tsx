import { getPolitician } from '@/lib/api'
import { formatBrazilianDate } from '@/lib/utils'
import type { PoliticianProfile, Confidence } from '@/lib/types'

interface PageProps {
  params: Promise<{ id: string }>
  searchParams: Promise<{ as_of?: string }>
}

function SnapshotBadge({ date }: { date: string | null }) {
  return (
    <div className="inline-flex items-center gap-2 text-xs text-white/50 bg-white/5 border border-white/10 rounded px-3 py-1.5">
      <span className="w-1.5 h-1.5 rounded-full bg-white/30 inline-block" />
      {date ? `Retrato em: ${formatBrazilianDate(date)}` : 'Dados atuais'}
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="border-t border-white/10 pt-8">
      <h2
        className="text-lg font-semibold text-[#f5f0e8] mb-4"
        style={{ fontFamily: 'var(--font-lora)' }}
      >
        {title}
      </h2>
      {children}
    </section>
  )
}

function Empty({ message }: { message: string }) {
  return <p className="text-sm text-white/40">{message}</p>
}

function ConfidenceBadge({ confidence }: { confidence: Confidence }) {
  if (confidence === 'high') return null
  const styles: Record<string, string> = {
    medium: 'bg-yellow-900/40 text-yellow-400',
    low: 'bg-red-900/40 text-red-400',
    uncertain: 'bg-white/5 text-white/30',
  }
  return (
    <span className={`ml-2 text-xs px-1.5 py-0.5 rounded ${styles[confidence] ?? ''}`}>
      {confidence}
    </span>
  )
}

function sourceYear(sourceLabel: string | null): string | null {
  if (!sourceLabel) return null
  const match = sourceLabel.match(/consulta_cand_(\d{4})/)
  return match ? match[1] : null
}

function ProfileContent({ profile, asOf }: { profile: PoliticianProfile; asOf?: string }) {
  const { person, party_affiliations, candidacies, mandates } = profile

  return (
    <div className="max-w-4xl mx-auto px-6 py-12 flex flex-col gap-10">
      {/* Identity block */}
      <div className="flex flex-col gap-3">
        <SnapshotBadge date={asOf ?? profile.snapshot_date} />
        <h1
          className="text-4xl font-semibold text-[#f5f0e8] mt-2"
          style={{ fontFamily: 'var(--font-lora)' }}
        >
          {person.canonical_name}
        </h1>
        {person.bio_summary && (
          <p className="text-sm text-white/60 leading-relaxed max-w-2xl">{person.bio_summary}</p>
        )}
        <div className="flex gap-6 text-xs text-white/40 mt-1">
          {person.birth_date && <span>Nascimento: {formatBrazilianDate(person.birth_date)}</span>}
          {person.death_date && <span>Falecimento: {formatBrazilianDate(person.death_date)}</span>}
          {person.gender && <span>{person.gender}</span>}
        </div>
      </div>

      {/* Party affiliations */}
      <Section title="Filiações partidárias">
        {party_affiliations.length === 0 ? (
          <Empty message="Filiações partidárias não disponíveis nesta fonte. Os dados de candidaturas TSE incluem o partido por candidatura." />
        ) : (
          <ul className="flex flex-col gap-3">
            {party_affiliations.map((aff) => (
              <li key={aff.id} className="text-sm text-white/70">
                <span className="font-mono text-white/30 text-xs">{aff.party_id}</span>
                <ConfidenceBadge confidence={aff.confidence} />
                {aff.notes && (
                  <span className="block text-xs text-white/30 mt-0.5">{aff.notes}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </Section>

      {/* Candidacies */}
      <Section title="Candidaturas">
        {candidacies.length === 0 ? (
          <Empty message="Sem candidaturas registradas." />
        ) : (
          <ul className="flex flex-col gap-3">
            {candidacies.map((c) => {
              const year = sourceYear(c.source_label)
              return (
                <li key={c.id} className="text-sm text-white/70">
                  {year && (
                    <span className="font-medium text-white/90 mr-2">{year}</span>
                  )}
                  {c.territory && (
                    <span>{c.territory}</span>
                  )}
                  {c.result && (
                    <span
                      className={`ml-2 text-xs px-1.5 py-0.5 rounded ${
                        c.result === 'elected'
                          ? 'bg-emerald-900/50 text-emerald-300'
                          : 'bg-white/5 text-white/40'
                      }`}
                    >
                      {c.result}
                    </span>
                  )}
                  <ConfidenceBadge confidence={c.confidence} />
                  {c.source_label && (
                    <span className="block text-xs text-white/20 mt-0.5">{c.source_label}</span>
                  )}
                </li>
              )
            })}
          </ul>
        )}
      </Section>

      {/* Mandates */}
      <Section title="Mandatos">
        {mandates.length === 0 ? (
          <Empty message="Mandatos não disponíveis nesta fonte. Os dados TSE cobrem candidaturas, não exercício do mandato." />
        ) : (
          <ul className="flex flex-col gap-3">
            {mandates.map((m) => (
              <li key={m.id} className="text-sm text-white/70">
                {m.territory && (
                  <span className="font-medium text-white/90">{m.territory}</span>
                )}
                {m.interrupted && (
                  <span className="ml-2 text-xs px-1.5 py-0.5 rounded bg-red-900/30 text-red-400">
                    interrompido{m.interruption_reason ? `: ${m.interruption_reason}` : ''}
                  </span>
                )}
                <ConfidenceBadge confidence={m.confidence} />
                {m.notes && (
                  <span className="block text-xs text-white/30 mt-0.5">{m.notes}</span>
                )}
              </li>
            ))}
          </ul>
        )}
      </Section>
    </div>
  )
}

export default async function PoliticianPage({ params, searchParams }: PageProps) {
  const { id } = await params
  const { as_of: asOf } = await searchParams

  let profile: PoliticianProfile | null = null
  let notFound = false

  try {
    profile = await getPolitician(id, asOf)
  } catch {
    notFound = true
  }

  if (notFound || !profile) {
    return (
      <div className="max-w-4xl mx-auto px-6 py-24 text-center">
        <p className="text-4xl text-white/20 mb-4" style={{ fontFamily: 'var(--font-lora)' }}>
          Não encontrado
        </p>
        <p className="text-sm text-white/40">
          Nenhum registro encontrado para o identificador <code className="text-white/60">{id}</code>.
        </p>
      </div>
    )
  }

  return <ProfileContent profile={profile} asOf={asOf} />
}
