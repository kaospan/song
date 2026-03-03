import { baseQuote } from './basePrompt'
import { segments, type SegmentName } from './segmentRegistry'

export function buildPrompt(segmentName: SegmentName) {
  const segment = segments[segmentName]

  return (
    baseQuote +
    `\n\nSEGMENT: ${segment.name}` +
    `\n\n${segment.segmentContent}` +
    `\n\n${segment.segmentSuffix}`
  )
}

export function buildPromptByName(segmentName: string) {
  const key = segmentName as SegmentName
  if (!segments[key]) {
    const available = Object.keys(segments).sort().join(', ')
    throw new Error(`Unknown prompt segment '${segmentName}'. Available: ${available}`)
  }
  return buildPrompt(key)
}

