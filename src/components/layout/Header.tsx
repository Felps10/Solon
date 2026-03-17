import Link from 'next/link'
import { NAV } from '@/lib/constants'

export default function Header() {
  return (
    <header
      className="sticky top-0 z-40 border-b"
      style={{
        background: 'var(--color-surface)',
        borderColor: 'var(--color-rule)',
      }}
    >
      <div className="max-w-5xl mx-auto px-6 py-4 flex items-center justify-between">
        <Link
          href="/"
          className="text-xl font-semibold tracking-tight transition-opacity hover:opacity-70"
          style={{ fontFamily: 'var(--font-display)', color: 'var(--color-ink)' }}
        >
          Sólon
        </Link>
        <nav className="flex items-center gap-8">
          <Link
            href="/"
            className="text-sm transition-colors"
            style={{ color: 'var(--color-muted)' }}
          >
            {NAV.search}
          </Link>
          <Link
            href="/sobre"
            className="text-sm transition-colors"
            style={{ color: 'var(--color-muted)' }}
          >
            {NAV.about}
          </Link>
        </nav>
      </div>
    </header>
  )
}
