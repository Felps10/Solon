export type DatePrecision = 'exact' | 'month' | 'year' | 'decade' | 'approximate' | 'unknown'
export type Confidence = 'high' | 'medium' | 'low' | 'uncertain'

export interface Person {
  id: string
  canonical_name: string
  birth_date: string | null
  death_date: string | null
  gender: string | null
  bio_summary: string | null
}

export interface PartyAffiliation {
  id: string
  party_id: string
  date_precision: DatePrecision
  is_approximate: boolean
  confidence: Confidence
  source_label: string | null
  notes: string | null
}

export interface Mandate {
  id: string
  office_id: string | null
  territory: string | null
  date_precision: DatePrecision
  is_approximate: boolean
  interrupted: boolean
  interruption_reason: string | null
  confidence: Confidence
  source_label: string | null
  notes: string | null
}

export interface Candidacy {
  id: string
  election_id: string
  office_id: string | null
  party_id: string | null
  territory: string | null
  result: string | null
  vote_count: number | null
  confidence: Confidence
  source_label: string | null
}

export interface PoliticianProfile {
  person: Person
  party_affiliations: PartyAffiliation[]
  candidacies: Candidacy[]
  mandates: Mandate[]
  snapshot_date: string | null
}
