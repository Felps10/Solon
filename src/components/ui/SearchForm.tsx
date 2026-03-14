'use client'

import { useState, FormEvent } from 'react'
import { useRouter } from 'next/navigation'

interface SearchFormProps {
  placeholder?: string
  className?: string
}

export default function SearchForm({ placeholder = 'Buscar político, partido ou cargo...', className = '' }: SearchFormProps) {
  const [query, setQuery] = useState('')
  const router = useRouter()

  function handleSubmit(e: FormEvent) {
    e.preventDefault()
    const trimmed = query.trim()
    if (!trimmed) return
    router.push(`/search?q=${encodeURIComponent(trimmed)}`)
  }

  return (
    <form onSubmit={handleSubmit} className={`flex w-full max-w-xl ${className}`}>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={placeholder}
        className="flex-1 bg-white/10 border border-white/20 text-white placeholder:text-white/40 px-4 py-3 text-sm rounded-l focus:outline-none focus:border-white/50 transition-colors"
      />
      <button
        type="submit"
        className="bg-white/10 hover:bg-white/20 border border-l-0 border-white/20 text-white/80 hover:text-white px-5 py-3 text-sm rounded-r transition-colors"
      >
        Buscar
      </button>
    </form>
  )
}
