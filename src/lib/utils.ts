export function formatBrazilianDate(dateStr: string | null): string {
  if (!dateStr) return 'Data desconhecida'
  const date = new Date(dateStr)
  return date.toLocaleDateString('pt-BR', { day: '2-digit', month: 'long', year: 'numeric' })
}

export function formatYear(dateStr: string | null): string {
  if (!dateStr) return '?'
  return new Date(dateStr).getFullYear().toString()
}

export function buildSnapshotUrl(politicianId: string, date: string): string {
  return `/politicians/${politicianId}?as_of=${date}`
}
