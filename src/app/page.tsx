import SearchForm from '@/components/ui/SearchForm'

const features = [
  {
    title: 'Perfil histórico',
    description: 'Veja a trajetória completa de qualquer figura política.',
  },
  {
    title: 'Linha do tempo',
    description: 'Navegue pelos eventos em ordem cronológica.',
  },
  {
    title: 'Retrato em data',
    description: 'Escolha uma data e veja o estado exato daquele momento.',
  },
]

export default function HomePage() {
  return (
    <div className="flex flex-col">
      {/* Hero */}
      <section className="flex flex-col items-center justify-center px-6 py-28 text-center border-b border-white/10">
        <h1
          className="text-5xl font-semibold leading-tight tracking-tight text-[#f5f0e8] max-w-2xl"
          style={{ fontFamily: 'var(--font-lora)' }}
        >
          A memória política do Brasil
        </h1>
        <p className="mt-5 text-base text-white/60 max-w-xl leading-relaxed">
          Trajetórias, mandatos e filiações de figuras políticas brasileiras desde 1889.
        </p>
        <div className="mt-10 w-full flex justify-center">
          <SearchForm />
        </div>
      </section>

      {/* Features */}
      <section className="max-w-6xl mx-auto w-full px-6 py-20">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-px bg-white/10 border border-white/10 rounded">
          {features.map((feature) => (
            <div key={feature.title} className="bg-[#0f1923] p-8">
              <h3
                className="text-lg font-semibold text-[#f5f0e8] mb-3"
                style={{ fontFamily: 'var(--font-lora)' }}
              >
                {feature.title}
              </h3>
              <p className="text-sm text-white/55 leading-relaxed">{feature.description}</p>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
