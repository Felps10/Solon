import type { CandidacySummary, PersonSummary } from '@/types/api'

// ─── Private helpers ─────────────────────────────────────────────────────────

function isFeminine(gender: string | null | undefined): boolean {
  return gender === 'feminino' || gender === 'F'
}

function normalizeTerritory(t: string): string {
  return t.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase()
}

function titleCaseBR(s: string): string {
  return s
    .split(' ')
    .map((word) => {
      if (word.length <= 2) return word
      return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
    })
    .join(' ')
}

const OFFICE_MAP: Record<string, { m: string; f: string }> = {
  'DEPUTADO FEDERAL':   { m: 'deputado federal',   f: 'deputada federal' },
  'DEPUTADO ESTADUAL':  { m: 'deputado estadual',  f: 'deputada estadual' },
  'DEPUTADO DISTRITAL': { m: 'deputado distrital', f: 'deputada distrital' },
  'VEREADOR':           { m: 'vereador',           f: 'vereadora' },
  'PREFEITO':           { m: 'prefeito',           f: 'prefeita' },
  'VICE-PREFEITO':      { m: 'vice-prefeito',      f: 'vice-prefeita' },
  'GOVERNADOR':         { m: 'governador',         f: 'governadora' },
  'VICE-GOVERNADOR':    { m: 'vice-governador',    f: 'vice-governadora' },
  'SENADOR':            { m: 'senador',            f: 'senadora' },
  'PRESIDENTE':         { m: 'presidente',         f: 'presidente' },
  'VICE-PRESIDENTE':    { m: 'vice-presidente',    f: 'vice-presidente' },
  'VICE PRESIDENTE':    { m: 'vice-presidente',    f: 'vice-presidente' },
}

function applyGender(rawOffice: string, feminine: boolean): string {
  const entry = OFFICE_MAP[rawOffice.toUpperCase()]
  if (!entry) return rawOffice.toLowerCase()
  return feminine ? entry.f : entry.m
}

// ─── Main export ─────────────────────────────────────────────────────────────

export function buildDeterministicBio(
  person: PersonSummary,
  candidacies: CandidacySummary[],
): string | null {
  // Filter to candidacies with a valid year, sort ascending
  const valid = candidacies
    .filter((c) => c.election_year != null)
    .sort((a, b) => a.election_year - b.election_year)

  if (valid.length === 0) return null

  const totalCount = valid.length
  const firstYear = valid[0].election_year
  const lastYear = valid[valid.length - 1].election_year
  const electedCount = valid.filter((c) => c.result === 'elected').length
  const feminine = isFeminine(person.gender)

  // ── Office frequency ──────────────────────────────────────────────────────
  const officeFreq = new Map<string, { display: string; count: number }>()
  for (const c of valid) {
    if (!c.office_name) continue
    const key = c.office_name.toUpperCase()
    const existing = officeFreq.get(key)
    if (existing) {
      existing.count++
    } else {
      officeFreq.set(key, { display: c.office_name, count: 1 })
    }
  }
  const topOffices = [...officeFreq.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 2)
    .map((v) => v.display)

  const hasFederal = topOffices.some((o) => {
    const u = o.toUpperCase()
    return (
      u.includes('FEDERAL') ||
      u === 'PRESIDENTE' ||
      u === 'VICE-PRESIDENTE' ||
      u === 'VICE PRESIDENTE' ||
      u === 'SENADOR'
    )
  })

  // ── Territory frequency (exclude BRASIL) ──────────────────────────────────
  const BRASIL_NORM = 'BRASIL'
  const territoryFreq = new Map<string, { display: string; count: number }>()
  for (const c of valid) {
    if (!c.territory) continue
    const norm = normalizeTerritory(c.territory)
    if (norm === BRASIL_NORM) continue
    const existing = territoryFreq.get(norm)
    if (existing) {
      existing.count++
      // Upgrade to the diacritic form if we have a plain form stored
      const storedNorm = normalizeTerritory(existing.display)
      if (existing.display === storedNorm && c.territory !== norm) {
        existing.display = c.territory
      }
    } else {
      territoryFreq.set(norm, { display: c.territory, count: 1 })
    }
  }
  const topTerritories = [...territoryFreq.values()]
    .sort((a, b) => b.count - a.count)
    .slice(0, 2)
    .map((v) => v.display)

  const allNational = topTerritories.length === 0

  // ── Sentence 1 (always) ───────────────────────────────────────────────────
  const politico = feminine ? 'Política' : 'Político'
  const nCand =
    totalCount === 1
      ? '1 candidatura registrada'
      : `${totalCount} candidaturas registradas`
  const period =
    firstYear === lastYear
      ? `em ${firstYear}`
      : `entre ${firstYear} e ${lastYear}`
  const s1 = `${politico} com ${nCand} pelo TSE ${period}.`

  // ── Sentence 2 (when we have office data) ────────────────────────────────
  let s2 = ''
  if (topOffices.length > 0) {
    const officeLabels = topOffices.map((o) => applyGender(o, feminine))
    const officeStr =
      officeLabels.length === 1
        ? officeLabels[0]
        : `${officeLabels[0]} e ${officeLabels[1]}`

    let territoryCont = ''
    if (allNational) {
      territoryCont = ', com alcance nacional'
    } else {
      const displayTerritories = topTerritories.map(titleCaseBR)
      const territoryStr =
        displayTerritories.length === 1
          ? displayTerritories[0]
          : `${displayTerritories[0]} e ${displayTerritories[1]}`
      if (hasFederal) {
        territoryCont = `, com atuação em ${territoryStr} e alcance nacional`
      } else {
        territoryCont = `, com atuação em ${territoryStr}`
      }
    }

    s2 = `Concorreu ao cargo de ${officeStr}${territoryCont}.`
  }

  // ── Sentence 3 (when elected at least once) ───────────────────────────────
  let s3 = ''
  if (electedCount > 0) {
    const eleitoA = feminine ? 'eleita' : 'eleito'
    const ocasiao = electedCount === 1 ? '1 ocasião' : `${electedCount} ocasiões`
    s3 = `Foi ${eleitoA} em ${ocasiao}.`
  }

  const parts = [s1]
  if (s2) parts.push(s2)
  if (s3) parts.push(s3)
  return parts.join(' ')
}
