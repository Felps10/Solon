import type { PersonDetail, CandidacySummary } from '@/types/api'
import { formatDate } from '@/lib/utils'
import { NULL_LABELS, PROFILE } from '@/lib/constants'
import NullState from '@/components/ui/NullState'
import { buildDeterministicBio } from '@/lib/bio'

interface IdentityHeaderProps {
  person: PersonDetail
  candidacies: CandidacySummary[]
}

export default function IdentityHeader({ person, candidacies }: IdentityHeaderProps) {
  const birthDate = person.birth_date ? formatDate(person.birth_date) : null
  const deathDate = person.death_date ? formatDate(person.death_date) : null

  return (
    <div className="pb-8 border-b" style={{ borderColor: 'var(--color-rule)' }}>
      <h1
        className="font-bold mb-2"
        style={{
          fontFamily: 'var(--font-display)',
          color: 'var(--color-ink)',
          fontSize: 'var(--text-4xl)',
        }}
      >
        {person.canonical_name}
      </h1>

      {(birthDate || deathDate || person.gender) && (
        <div
          className="flex flex-wrap items-center gap-5 text-sm mt-1"
          style={{ color: 'var(--color-muted)' }}
        >
          {birthDate && (
            <span>
              <span className="font-medium">{PROFILE.birth_date}:</span> {birthDate}
            </span>
          )}
          {deathDate && (
            <span>
              <span className="font-medium">{PROFILE.death_date}:</span> {deathDate}
            </span>
          )}
          {person.gender && <span>{person.gender}</span>}
        </div>
      )}

      <div className="mt-4">
        {(() => {
          const bio = person.bio_summary ?? buildDeterministicBio(person, candidacies)
          if (!bio) return <NullState label={NULL_LABELS.bio} />
          return (
            <>
              <p className="text-sm text-white/60 leading-relaxed max-w-2xl">{bio}</p>
              {!person.bio_summary && (
                <p className="text-xs text-white/25 mt-1">
                  Gerado automaticamente a partir de dados estruturados do TSE.
                </p>
              )}
            </>
          )
        })()}
      </div>
    </div>
  )
}
