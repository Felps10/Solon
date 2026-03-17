import { redirect } from 'next/navigation'

interface PageProps {
  searchParams: Promise<{ q?: string }>
}

export default async function SearchPage({ searchParams }: PageProps) {
  const { q } = await searchParams
  redirect(q ? `/?q=${encodeURIComponent(q)}` : '/')
}
