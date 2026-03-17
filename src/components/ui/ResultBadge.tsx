import { getResultDisplay } from '@/lib/constants'

interface ResultBadgeProps {
  result: string | null | undefined
  className?: string
}

export default function ResultBadge({ result, className = '' }: ResultBadgeProps) {
  const { label, colorClass } = getResultDisplay(result)
  return (
    <span className={`text-xs px-1.5 py-0.5 rounded ${colorClass} ${className}`}>
      {label}
    </span>
  )
}
