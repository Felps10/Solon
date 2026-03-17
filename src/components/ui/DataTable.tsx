'use client'

import { useState } from 'react'

export interface Column<T> {
  key: keyof T | string
  header: string
  render?: (row: T) => React.ReactNode
  sortable?: boolean
  align?: 'left' | 'right' | 'center'
}

interface DataTableProps<T> {
  columns: Column<T>[]
  rows: T[]
  rowKey: (row: T) => string
  emptyMessage?: string
  rowClassName?: (row: T) => string | undefined
}

type SortDir = 'asc' | 'desc'

export default function DataTable<T>({
  columns,
  rows,
  rowKey,
  emptyMessage = 'Nenhum registro encontrado.',
  rowClassName,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<SortDir>('asc')

  function handleSort(key: string) {
    if (sortKey === key) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const sorted = sortKey
    ? [...rows].sort((a, b) => {
        const av = (a as Record<string, unknown>)[sortKey]
        const bv = (b as Record<string, unknown>)[sortKey]
        if (av == null && bv == null) return 0
        if (av == null) return 1
        if (bv == null) return -1
        const cmp = av < bv ? -1 : av > bv ? 1 : 0
        return sortDir === 'asc' ? cmp : -cmp
      })
    : rows

  return (
    <div className="overflow-x-auto w-full">
      <table
        className="w-full text-sm border-collapse"
        style={{ color: 'var(--color-ink)' }}
      >
        <thead>
          <tr style={{ borderBottom: '2px solid var(--color-rule)' }}>
            {columns.map((col) => (
              <th
                key={String(col.key)}
                className={`py-2 px-3 font-semibold whitespace-nowrap text-${col.align ?? 'left'}`}
                style={{ color: 'var(--color-muted)', fontSize: 'var(--text-xs)' }}
              >
                {col.sortable ? (
                  <button
                    type="button"
                    onClick={() => handleSort(String(col.key))}
                    className="inline-flex items-center gap-1 hover:opacity-70 transition-opacity"
                  >
                    {col.header}
                    <span aria-hidden="true" className="text-xs">
                      {sortKey === col.key ? (sortDir === 'asc' ? '↑' : '↓') : '⇅'}
                    </span>
                  </button>
                ) : (
                  col.header
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.length === 0 ? (
            <tr>
              <td
                colSpan={columns.length}
                className="py-8 px-3 text-center italic"
                style={{ color: 'var(--color-muted)' }}
              >
                {emptyMessage}
              </td>
            </tr>
          ) : (
            sorted.map((row) => (
              <tr
                key={rowKey(row)}
                className={`transition-colors hover:bg-black/2 ${rowClassName?.(row) ?? ''}`}
                style={{ borderBottom: '1px solid var(--color-rule)' }}
              >
                {columns.map((col) => (
                  <td
                    key={String(col.key)}
                    className={`py-2.5 px-3 align-top text-${col.align ?? 'left'}`}
                  >
                    {col.render
                      ? col.render(row)
                      : String((row as Record<string, unknown>)[String(col.key)] ?? '')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}
