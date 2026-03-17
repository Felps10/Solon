import type { ConfidenceRaw, HitKindRaw, HitKindLabel } from '@/types/api'

// ─── Null / empty state labels ────────────────────────────────────────────────

export const NULL_LABELS = {
  date: 'Data desconhecida',
  territory: 'Território não informado',
  party: 'Partido não informado',
  office: 'Cargo não informado',
  result: 'Resultado não informado',
  vote_count: 'Votos não informados',
  nome_urna: 'Nome de urna não informado',
  bio: 'Sem biografia disponível',
  generic: 'Não informado',
} as const

// ─── Confidence ───────────────────────────────────────────────────────────────

export const CONFIDENCE_LABELS: Record<ConfidenceRaw, string> = {
  high:      'Alta',
  medium:    'Média',
  low:       'Baixa',
  uncertain: 'Incerta',
}

export const CONFIDENCE_TOOLTIPS: Record<ConfidenceRaw, string> = {
  high:      'Dado com alta confiabilidade, confirmado por múltiplas fontes',
  medium:    'Dado com confiabilidade média, confirmado por uma fonte primária',
  low:       'Dado com baixa confiabilidade, requer verificação adicional',
  uncertain: 'Dado incerto, pode conter erros',
}

// ─── Search hit kinds ─────────────────────────────────────────────────────────

export const HIT_KIND_LABELS: Record<HitKindRaw, HitKindLabel> = {
  person:    'pessoa',
  party:     'partido',
  office:    'cargo',
  candidacy: 'candidatura',
}

export const HIT_KIND_DISPLAY: Record<HitKindLabel, string> = {
  pessoa:       'Pessoa',
  partido:      'Partido',
  cargo:        'Cargo',
  candidatura:  'Candidatura',
}

// ─── Election results ─────────────────────────────────────────────────────────

export type CandidacyResult =
  | 'elected'
  | 'defeated'
  | 'annulled'
  | 'renounced'
  | 'unknown'

export interface ResultDisplay {
  label: string
  colorClass: string
}

export const RESULT_DISPLAY: Record<CandidacyResult, ResultDisplay> = {
  elected: {
    label:      'Eleito',
    colorClass: 'bg-teal-900/40 text-teal-300',
  },
  defeated: {
    label:      'Derrotado',
    colorClass: 'bg-white/5 text-white/40',
  },
  annulled: {
    label:      'Cassado',
    colorClass: 'bg-red-900/30 text-red-400',
  },
  renounced: {
    label:      'Renúncia',
    colorClass: 'bg-amber-900/30 text-amber-400',
  },
  unknown: {
    label:      'Desconhecido',
    colorClass: 'bg-white/5 text-white/20',
  },
}

export function getResultDisplay(
  result: string | null | undefined
): ResultDisplay {
  if (!result) return RESULT_DISPLAY.unknown
  return RESULT_DISPLAY[result as CandidacyResult] ?? RESULT_DISPLAY.unknown
}

/** @deprecated use RESULT_DISPLAY instead */
export const RESULT_LABELS: Record<string, string> = Object.fromEntries(
  Object.entries(RESULT_DISPLAY).map(([k, v]) => [k, v.label])
)

// ─── Reference data ───────────────────────────────────────────────────────────

export const ELECTION_YEARS = [
  2024, 2022, 2020, 2018, 2016, 2014, 2012, 2010,
  2008, 2006, 2004, 2002, 2000, 1998, 1996, 1994,
  1992, 1990, 1988, 1986,
] as const

export const UF_LIST = [
  'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO',
  'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR',
  'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO',
] as const

// ─── Navigation ───────────────────────────────────────────────────────────────

export const NAV = {
  search: 'Buscar',
  about:  'Sobre',
  home:   'Início',
} as const

// ─── Table headers ────────────────────────────────────────────────────────────

export const TABLE_HEADERS = {
  year:       'Ano',
  office:     'Cargo',
  party:      'Partido',
  territory:  'Território',
  result:     'Resultado',
  votes:      'Votos',
  nome_urna:  'Nome de Urna',
  name:       'Nome',
  confidence: 'Confiabilidade',
  source:     'Fonte',
} as const

// ─── Aria labels ──────────────────────────────────────────────────────────────

export const ARIA_LABELS = {
  search_input:   'Buscar pessoas, partidos ou cargos',
  search_button:  'Realizar busca',
  clear_search:   'Limpar busca',
  filter_type:    'Filtrar por tipo',
  filter_year:    'Filtrar por ano',
  filter_uf:      'Filtrar por estado',
  filter_party:   'Filtrar por partido',
  load_more:      'Carregar mais resultados',
  close_drawer:   'Fechar painel',
  evidence_drawer:'Painel de evidências',
} as const

// ─── Search UI strings ────────────────────────────────────────────────────────

export const SEARCH = {
  placeholder:       'Buscar políticos, partidos, cargos…',
  results_singular:  'resultado',
  results_plural:    'resultados',
  no_results:        'Nenhum resultado encontrado',
  no_results_hint:   'Tente outros termos ou remova filtros',
  loading:           'Buscando…',
  load_more:         'Carregar mais',
  type_label:        'Tipo',
  year_label:        'Ano',
  uf_label:          'Estado',
  party_label:       'Partido',
  all_types:         'Todos os tipos',
  all_years:         'Todos os anos',
  all_ufs:           'Todos os estados',
  all_parties:       'Todos os partidos',
} as const

// ─── Profile UI strings ───────────────────────────────────────────────────────

export const PROFILE = {
  candidacies_title:  'Candidaturas',
  snapshot_label:     'Ver perfil em',
  snapshot_presets:   'Momentos históricos',
  birth_date:         'Nascimento',
  death_date:         'Falecimento',
  evidence:           'Evidências',
  sources:            'Fontes',
  back_to_search:     '← Voltar à busca',
} as const

// ─── Snapshot date presets ────────────────────────────────────────────────────

export const SNAPSHOT_PRESETS: { label: string; date: string }[] = [
  { label: 'Antes de 2022', date: '2022-09-30' },
  { label: 'Antes de 2018', date: '2018-09-30' },
  { label: 'Antes de 2014', date: '2014-09-30' },
  { label: 'Antes de 2010', date: '2010-09-30' },
  { label: 'Antes de 2006', date: '2006-09-30' },
  { label: 'Antes de 2002', date: '2002-09-30' },
]
