import { redirect } from 'next/navigation'

interface PageProps {
  params: Promise<{ id: string }>
  searchParams: Promise<{ as_of?: string }>
}

export default async function PoliticianLegacyPage({ params, searchParams }: PageProps) {
  const { id } = await params
  const { as_of } = await searchParams
  redirect(as_of ? `/pessoas/${id}?as_of=${as_of}` : `/pessoas/${id}`)
}
