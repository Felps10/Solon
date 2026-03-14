import axios from 'axios'
import type { PoliticianProfile, Person } from './types'

const client = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  headers: { 'Content-Type': 'application/json' },
})

export async function getPolitician(id: string, asOf?: string): Promise<PoliticianProfile> {
  const { data } = await client.get(`/people/${id}`, {
    params: asOf ? { as_of: asOf } : {},
  })
  return data
}

export async function searchPoliticians(query: string): Promise<Person[]> {
  if (query.length < 2) return []
  const { data } = await client.get('/people/search', { params: { q: query } })
  return data
}
