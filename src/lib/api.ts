import type {
  PaginatedResponse,
  PersonDetail,
  PersonSummary,
  CandidacyDetail,
  SearchResponse,
  SearchHit,
  PersonHit,
  PartyHit,
  OfficeHit,
  CandidacyHit,
  HitKindRaw,
  HitKindLabel,
  SearchQueryParams,
} from '@/types/api'
import { HIT_KIND_LABELS } from '@/lib/constants'

// Raw hit shapes as returned by the API before kind normalization
type SearchHitRaw =
  | ({ kind: 'person' }    & Omit<PersonHit,    'kind'>)
  | ({ kind: 'party' }     & Omit<PartyHit,     'kind'>)
  | ({ kind: 'office' }    & Omit<OfficeHit,    'kind'>)
  | ({ kind: 'candidacy' } & Omit<CandidacyHit, 'kind'>)

const BASE = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

async function apiFetch<T>(
  path: string,
  params?: Record<string, string | number | boolean | undefined>,
): Promise<T> {
  const url = new URL(`${BASE}${path}`)
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, String(value))
      }
    }
  }
  const res = await fetch(url.toString(), { cache: 'no-store' })
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${path}`)
  }
  return res.json() as Promise<T>
}

function normalizeHit(hit: SearchHitRaw): SearchHit {
  return { ...hit, kind: HIT_KIND_LABELS[hit.kind] } as SearchHit
}

// ─── Search ──────────────────────────────────────────────────────────────────

export async function searchPeople(
  q: string,
  params?: Omit<SearchQueryParams, 'q'>,
): Promise<SearchResponse> {
  if (q.trim().length < 2) {
    return {
      hits: [],
      total: 0,
      facets: { tipo: [], uf: [], ano: [], partido: [] },
      query: q,
    }
  }

  // translate PT-BR kind label back to raw API value
  const kindRaw: HitKindRaw | undefined = params?.tipo
    ? (Object.entries(HIT_KIND_LABELS).find(([, v]) => v === params.tipo)?.[0] as HitKindRaw)
    : undefined

  // convert limit/offset pagination to page/page_size
  const pageSize = params?.limit ?? 20
  const page = params?.offset != null && params.offset > 0
    ? Math.floor(params.offset / pageSize) + 1
    : 1

  const raw = await apiFetch<{
    hits: SearchHitRaw[]
    query: string
    meta: { total: number; page: number; page_size: number; has_next: boolean }
  }>('/api/v1/search', {
    q,
    kinds:     kindRaw,
    page_size: pageSize,
    page,
  })

  return {
    hits:   raw.hits.map(normalizeHit),
    total:  raw.meta.total,
    facets: { tipo: [], uf: [], ano: [], partido: [] },
    query:  raw.query,
  }
}

// ─── People ──────────────────────────────────────────────────────────────────

export async function getPerson(id: string, asOf?: string): Promise<PersonDetail> {
  return apiFetch<PersonDetail>(
    `/api/v1/people/${id}`,
    asOf ? { as_of: asOf } : undefined,
  )
}

export async function listPeopleSearch(
  q: string,
  params?: { limit?: number; offset?: number },
): Promise<PaginatedResponse<PersonSummary>> {
  return apiFetch<PaginatedResponse<PersonSummary>>('/api/v1/people/search', { q, ...params })
}

// ─── Candidacies ─────────────────────────────────────────────────────────────

export async function listCandidacies(params?: {
  year?: number
  office?: string
  territory?: string
  party?: string
  result?: string
  limit?: number
  offset?: number
}): Promise<PaginatedResponse<CandidacyDetail>> {
  return apiFetch<PaginatedResponse<CandidacyDetail>>('/api/v1/candidacies', params)
}
