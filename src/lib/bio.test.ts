import { describe, it, expect } from 'vitest'
import { buildDeterministicBio } from './bio'
import type { CandidacySummary, PersonSummary } from '@/types/api'

// ─── Test helpers ─────────────────────────────────────────────────────────────

function cand(overrides: Omit<Partial<CandidacySummary>, 'office_name'> & { office_name?: string | null }): CandidacySummary {
  return {
    id: 'test-id',
    election_year: 2022,
    office_name: 'VEREADOR',
    party_abbr: null,
    territory: 'CAMPINAS',
    result: 'defeated',
    vote_count: null,
    nome_urna: null,
    confidence: 'high',
    source_label: null,
    ...overrides,
  } as CandidacySummary
}

function person(overrides: Partial<PersonSummary>): PersonSummary {
  return {
    id: 'test-person',
    canonical_name: 'TESTE DA SILVA',
    birth_date: null,
    death_date: null,
    gender: 'masculino',
    bio_summary: null,
    ...overrides,
  }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('buildDeterministicBio', () => {
  it('returns null when candidacies is empty', () => {
    expect(buildDeterministicBio(person({}), [])).toBe(null)
  })

  it('masculino, single candidacy, not elected, federal office, BRASIL territory', () => {
    const result = buildDeterministicBio(
      person({ gender: 'masculino' }),
      [cand({ office_name: 'DEPUTADO FEDERAL', territory: 'BRASIL', result: 'defeated' })],
    )
    expect(result).toContain('Político')
    expect(result).toContain('deputado federal')
    expect(result).toContain('alcance nacional')
    expect(result).not.toContain('Foi eleito')
    expect(result).not.toContain('Brasil')
  })

  it('feminino, single candidacy, elected once, municipal office', () => {
    const result = buildDeterministicBio(
      person({ gender: 'feminino' }),
      [cand({ election_year: 2020, office_name: 'VEREADOR', territory: 'CAMPINAS', result: 'elected' })],
    )
    expect(result).toContain('Política')
    expect(result).toContain('vereadora')
    expect(result).toContain('Campinas')
    expect(result).toContain('Foi eleita em 1 ocasião')
  })

  it("multiple candidacies in same year uses 'em' not 'entre'", () => {
    const result = buildDeterministicBio(
      person({}),
      [cand({ election_year: 2022 }), cand({ election_year: 2022 })],
    )
    expect(result).toContain('em 2022')
    expect(result).not.toContain('entre')
  })

  it("candidacies across different years uses 'entre'", () => {
    const result = buildDeterministicBio(
      person({}),
      [cand({ election_year: 2014 }), cand({ election_year: 2022 })],
    )
    expect(result).toContain('entre 2014 e 2022')
  })

  it('PRESIDENTE with BRASIL territory produces only alcance nacional', () => {
    const result = buildDeterministicBio(
      person({}),
      [
        cand({ office_name: 'PRESIDENTE', territory: 'BRASIL' }),
        cand({ office_name: 'PRESIDENTE', territory: 'BRASIL' }),
        cand({ office_name: 'PRESIDENTE', territory: 'BRASIL' }),
      ],
    )
    expect(result).toContain('alcance nacional')
    expect(result).not.toContain('Brasil')
    expect(result).not.toContain('BRASIL')
  })

  it('federal deputy uses state territory and adds alcance nacional', () => {
    const result = buildDeterministicBio(
      person({}),
      [cand({ office_name: 'DEPUTADO FEDERAL', territory: 'SÃO PAULO' })],
    )
    expect(result).toContain('São Paulo')
    expect(result).toContain('alcance nacional')
  })

  it('mixed state and federal offices combines territory and alcance nacional', () => {
    const result = buildDeterministicBio(
      person({}),
      [
        cand({ office_name: 'DEPUTADO ESTADUAL', territory: 'SÃO PAULO', result: 'elected' }),
        cand({ office_name: 'DEPUTADO FEDERAL', territory: 'SÃO PAULO', result: 'elected' }),
      ],
    )
    expect(result).toContain('São Paulo')
    expect(result).toContain('alcance nacional')
  })

  it('null gender defaults to masculine without throwing', () => {
    const result = buildDeterministicBio(
      person({ gender: null }),
      [cand({ office_name: 'VEREADOR', territory: 'CAMPINAS' })],
    )
    expect(result).toContain('Político')
    expect(result).toContain('vereador')
  })

  it("legacy gender '#ne' defaults to masculine without throwing", () => {
    const result = buildDeterministicBio(
      person({ gender: '#ne' }),
      [cand({ office_name: 'GOVERNADOR', territory: 'BAHIA' })],
    )
    expect(result).toContain('Político')
    expect(result).toContain('governador')
  })

  it('all office_name null omits the office sentence', () => {
    const result = buildDeterministicBio(
      person({}),
      [cand({ office_name: null }), cand({ office_name: null })],
    )
    expect(result).not.toContain('Concorreu')
  })

  it('three elected candidacies produces plural ocasiões', () => {
    const result = buildDeterministicBio(
      person({}),
      [
        cand({ result: 'elected' }),
        cand({ result: 'elected' }),
        cand({ result: 'elected' }),
      ],
    )
    expect(result).toContain('3 ocasiões')
  })

  it('diacritic deduplication treats SÃO PAULO and SAO PAULO as the same territory', () => {
    const result = buildDeterministicBio(
      person({}),
      [
        cand({ office_name: 'VEREADOR', territory: 'SÃO PAULO' }),
        cand({ office_name: 'VEREADOR', territory: 'SAO PAULO' }),
        cand({ office_name: 'VEREADOR', territory: 'SAO PAULO' }),
      ],
    )
    // Count occurrences of "São Paulo"
    const matches = result?.match(/São Paulo/g)
    expect(matches).toHaveLength(1)
    expect(result).not.toContain('Sao Paulo')
  })
})
