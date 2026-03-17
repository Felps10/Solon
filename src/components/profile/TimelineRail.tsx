'use client'

import { useRouter } from 'next/navigation'
import { useEffect, useRef } from 'react'
import type { CandidacySummary } from '@/types/api'
import { getPartyColor } from '@/lib/utils'
import { getResultDisplay } from '@/lib/constants'

interface TimelineRailProps {
  candidacies: CandidacySummary[]
  activeYear: number | null
  personId: string
}

interface YearNode {
  year: number
  elected: boolean
  officeName: string | null
  partyAbbr: string | null
  result: string | null
}

function abbreviateOffice(name: string): string {
  const map: Record<string, string> = {
    'DEPUTADO FEDERAL':  'Dep. Federal',
    'DEPUTADO ESTADUAL': 'Dep. Estadual',
    'SENADOR':           'Senador',
    'PRESIDENTE':        'Presidente',
    'GOVERNADOR':        'Governador',
    'PREFEITO':          'Prefeito',
    'VEREADOR':          'Vereador',
  }
  return map[name.toUpperCase()] ?? name
}

export default function TimelineRail({ candidacies, activeYear, personId }: TimelineRailProps) {
  const router = useRouter()
  const activeRef = useRef<HTMLButtonElement>(null)

  // Build one node per distinct election year
  const yearMap = new Map<number, CandidacySummary[]>()
  for (const c of candidacies) {
    const list = yearMap.get(c.election_year) ?? []
    list.push(c)
    yearMap.set(c.election_year, list)
  }

  const nodes: YearNode[] = Array.from(yearMap.entries())
    .sort(([a], [b]) => a - b)
    .map(([year, cs]) => ({
      year,
      elected:    cs.some(c => c.result === 'elected'),
      officeName: cs[0]?.office_name ?? null,
      partyAbbr:  cs[0]?.party_abbr ?? null,
      result:     cs[0]?.result ?? null,
    }))

  const activeIndex = nodes.findIndex(n => n.year === activeYear)

  function goToYear(year: number) {
    router.push(`/pessoas/${personId}?as_of=${year}-12-31`)
  }

  function reset() {
    router.push(`/pessoas/${personId}`)
  }

  function handleKeyDown(e: React.KeyboardEvent, index: number) {
    if (e.key === 'ArrowRight' && index < nodes.length - 1) {
      goToYear(nodes[index + 1].year)
    } else if (e.key === 'ArrowLeft' && index > 0) {
      goToYear(nodes[index - 1].year)
    }
  }

  // Scroll active node into view on mount / year change
  useEffect(() => {
    activeRef.current?.scrollIntoView({ block: 'nearest', inline: 'center', behavior: 'smooth' })
  }, [activeYear])

  if (nodes.length === 0) return null

  return (
    <div className="mb-8" role="navigation" aria-label="Linha do tempo de candidaturas">
      {/* Reset chip */}
      <div className="flex items-center gap-2 mb-4">
        <button
          type="button"
          onClick={reset}
          className="text-xs px-3 py-1 rounded-full border transition-colors"
          style={{
            borderColor: activeYear === null ? 'var(--color-ink)' : 'var(--color-rule)',
            color:       activeYear === null ? 'var(--color-ink)' : 'var(--color-muted)',
          }}
          aria-pressed={activeYear === null}
        >
          Dados atuais
        </button>
      </div>

      {/* Scrollable rail */}
      <div className="overflow-x-auto -mx-1 px-1 pb-2">
        <div className="flex items-start min-w-max">
          {nodes.map((node, i) => {
            const isActive    = node.year === activeYear
            const partyColor  = getPartyColor(node.partyAbbr)

            return (
              <div key={node.year} className="flex items-center">
                {/* Connector line between nodes */}
                {i > 0 && (
                  <div
                    className="w-6 h-px flex-shrink-0 mt-4"
                    style={{ background: 'var(--color-rule)' }}
                  />
                )}

                {/* Year node button */}
                <button
                  ref={isActive ? activeRef : undefined}
                  type="button"
                  onClick={() => goToYear(node.year)}
                  onKeyDown={(e) => handleKeyDown(e, i)}
                  aria-pressed={isActive}
                  aria-label={`${node.year}${node.officeName ? `: ${node.officeName}` : ''}`}
                  className="flex flex-col items-center gap-1.5 px-2 py-2 rounded-xl transition-colors focus:outline-none focus-visible:ring-2"
                  style={{
                    background:  isActive ? 'var(--color-surface)' : 'transparent',
                    outlineColor: 'var(--color-accent)',
                  }}
                >
                  {/* Circle dot */}
                  <div
                    className="w-4 h-4 rounded-full border-2 transition-all"
                    style={{
                      borderColor: partyColor,
                      background:  node.elected
                        ? partyColor
                        : isActive
                          ? 'var(--color-surface)'
                          : 'transparent',
                      boxShadow: isActive
                        ? `0 0 0 2px var(--color-paper), 0 0 0 4px ${partyColor}`
                        : 'none',
                    }}
                  />

                  {/* Year label */}
                  <span
                    className="text-xs"
                    style={{
                      fontFamily: 'var(--font-mono)',
                      color:      isActive ? 'var(--color-ink)' : 'var(--color-muted)',
                      fontWeight: isActive ? 600 : 400,
                    }}
                  >
                    {node.year}
                  </span>

                  {/* Abbreviated office */}
                  {node.officeName && (
                    <span
                      className="text-center leading-tight max-w-[72px]"
                      style={{
                        fontSize: '0.625rem',
                        color:    isActive ? 'var(--color-ink)' : 'var(--color-muted)',
                      }}
                    >
                      {abbreviateOffice(node.officeName)}
                    </span>
                  )}

                  {/* Result badge */}
                  {(() => {
                    const { label, colorClass } = getResultDisplay(node.result)
                    return (
                      <span
                        className={`px-1.5 py-px rounded ${colorClass}`}
                        style={{ fontSize: '0.6rem' }}
                      >
                        {label}
                      </span>
                    )
                  })()}
                </button>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
