import { Suspense } from 'react'
import SearchPageClient from '@/components/search/SearchPageClient'

export default function HomePage() {
  return (
    <Suspense>
      <SearchPageClient />
    </Suspense>
  )
}
