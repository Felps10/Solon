import type { ConfidenceRaw } from '@/types/api'
import { CONFIDENCE_LABELS } from '@/lib/constants'

// ─── Date formatting ──────────────────────────────────────────────────────────

export function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return ''
  // append noon UTC to avoid timezone shift on date-only strings
  const d = new Date(dateStr.length === 10 ? `${dateStr}T12:00:00Z` : dateStr)
  return d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'long', year: 'numeric' })
}

export function formatDateShort(dateStr: string | null | undefined): string {
  if (!dateStr) return ''
  const d = new Date(dateStr.length === 10 ? `${dateStr}T12:00:00Z` : dateStr)
  return d.toLocaleDateString('pt-BR', { month: 'short', year: 'numeric' })
}

export function formatDateTime(dateStr: string | null | undefined): string {
  if (!dateStr) return ''
  return new Date(dateStr).toLocaleString('pt-BR')
}

// ─── Number formatting ────────────────────────────────────────────────────────

export function formatVotes(count: number | null | undefined): string {
  if (count == null) return ''
  return count.toLocaleString('pt-BR')
}

// ─── Confidence helpers ───────────────────────────────────────────────────────

export function getConfidenceLabel(confidence: ConfidenceRaw): string {
  return CONFIDENCE_LABELS[confidence] ?? confidence
}

export function getConfidenceColor(confidence: ConfidenceRaw): string {
  switch (confidence) {
    case 'high':     return 'var(--color-confidence-high)'
    case 'medium':   return 'var(--color-confidence-medium)'
    case 'low':      return 'var(--color-confidence-low)'
    default:         return 'var(--color-muted)'
  }
}

export function getConfidenceBgColor(confidence: ConfidenceRaw): string {
  switch (confidence) {
    case 'high':     return 'var(--color-confidence-high-bg)'
    case 'medium':   return 'var(--color-confidence-medium-bg)'
    case 'low':      return 'var(--color-confidence-low-bg)'
    default:         return 'var(--color-rule)'
  }
}

// ─── Party colors ─────────────────────────────────────────────────────────────

const PARTY_COLORS: Record<string, string> = {
  PT:           'var(--color-party-pt)',
  PSDB:         'var(--color-party-psdb)',
  PL:           'var(--color-party-pl)',
  PP:           'var(--color-party-pp)',
  MDB:          'var(--color-party-mdb)',
  PMDB:         'var(--color-party-mdb)',
  PSD:          'var(--color-party-psd)',
  REPUBLICANOS: 'var(--color-party-republicans)',
  UNIÃO:        'var(--color-party-union)',
  UNION:        'var(--color-party-union)',
}

export function getPartyColor(abbr: string | null | undefined): string {
  if (!abbr) return 'var(--color-party-default)'
  return PARTY_COLORS[abbr.toUpperCase()] ?? 'var(--color-party-default)'
}

// ─── URL builders ─────────────────────────────────────────────────────────────

export function buildPersonUrl(id: string): string {
  return `/pessoas/${id}`
}

export function buildSnapshotUrl(personId: string, date: string): string {
  return `/pessoas/${personId}?as_of=${date}`
}

export function yearToDateRange(year: number): { start: string; end: string } {
  return { start: `${year}-01-01`, end: `${year}-12-31` }
}

// ─── CSS class merging ────────────────────────────────────────────────────────

export function cn(...classes: (string | undefined | null | false)[]): string {
  return classes.filter(Boolean).join(' ')
}
