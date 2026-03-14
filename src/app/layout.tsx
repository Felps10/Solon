import type { Metadata } from 'next'
import { Lora, Inter } from 'next/font/google'
import './globals.css'
import Header from '@/components/layout/Header'
import Footer from '@/components/layout/Footer'

const lora = Lora({
  variable: '--font-lora',
  subsets: ['latin'],
  display: 'swap',
})

const inter = Inter({
  variable: '--font-inter',
  subsets: ['latin'],
  display: 'swap',
})

export const metadata: Metadata = {
  title: 'Sólon — Memória Política Brasileira',
  description: 'Trajetórias, mandatos e filiações de figuras políticas brasileiras desde 1889.',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="pt-BR" className={`${lora.variable} ${inter.variable}`}>
      <body className="flex flex-col min-h-screen antialiased">
        <Header />
        <main className="flex-1 w-full">
          {children}
        </main>
        <Footer />
      </body>
    </html>
  )
}
