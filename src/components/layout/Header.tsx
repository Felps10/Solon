import Link from 'next/link'

export default function Header() {
  return (
    <header className="bg-[#0f1923] text-white border-b border-white/10">
      <div className="max-w-6xl mx-auto px-6 py-4 flex items-center justify-between">
        <Link
          href="/"
          className="text-xl font-semibold tracking-tight hover:text-white/80 transition-colors"
          style={{ fontFamily: 'var(--font-lora)' }}
        >
          Sólon
        </Link>
        <nav className="flex items-center gap-8">
          <Link
            href="/search"
            className="text-sm text-white/70 hover:text-white transition-colors"
          >
            Buscar
          </Link>
          <Link
            href="/about"
            className="text-sm text-white/70 hover:text-white transition-colors"
          >
            Sobre
          </Link>
        </nav>
      </div>
    </header>
  )
}
