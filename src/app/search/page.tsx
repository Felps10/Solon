import Link from 'next/link'
import { searchPoliticians } from '@/lib/api'
import { formatBrazilianDate } from '@/lib/utils'

interface PageProps {
  searchParams: Promise<{ q?: string }>
}

export default async function SearchPage({ searchParams }: PageProps) {
  const { q } = await searchParams
  const query = q?.trim() ?? ''

  const results = query.length >= 2 ? await searchPoliticians(query) : []

  return (
    <div className="max-w-4xl mx-auto px-6 py-12">
      <h1
        className="text-3xl font-semibold text-[#f5f0e8] mb-2"
        style={{ fontFamily: 'var(--font-lora)' }}
      >
        Resultados da busca
      </h1>

      {query ? (
        <p className="text-sm text-white/40 mb-8">
          {results.length > 0
            ? `${results.length} resultado${results.length !== 1 ? 's' : ''} para "${query}"`
            : query.length < 2
            ? 'Digite ao menos 2 caracteres para buscar.'
            : `Nenhum resultado para "${query}"`}
        </p>
      ) : (
        <p className="text-sm text-white/40 mb-8">Nenhum termo de busca informado.</p>
      )}

      {results.length > 0 && (
        <ul className="flex flex-col divide-y divide-white/5">
          {results.map((person) => (
            <li key={person.id}>
              <Link
                href={`/politicians/${person.id}`}
                className="flex flex-col gap-1 py-4 px-2 hover:bg-white/5 rounded transition-colors"
              >
                <span className="text-base font-medium text-[#f5f0e8]">
                  {person.canonical_name}
                </span>
                <span className="text-xs text-white/40">
                  {person.birth_date
                    ? `Nascimento: ${formatBrazilianDate(person.birth_date)}`
                    : 'Data de nascimento desconhecida'}
                  {person.gender ? ` · ${person.gender}` : ''}
                </span>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
