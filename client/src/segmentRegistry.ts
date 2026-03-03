import type { PromptSegment } from './segments/types'

import { segment as cinematicStudio } from './segments/cinematic_studio'
import { segment as highEnergy } from './segments/high_energy'
import { segment as intimateAcoustic } from './segments/intimate_acoustic'
import { segment as noirMinimal } from './segments/noir_minimal'

export const segments = {
  [cinematicStudio.name]: cinematicStudio,
  [intimateAcoustic.name]: intimateAcoustic,
  [highEnergy.name]: highEnergy,
  [noirMinimal.name]: noirMinimal,
} satisfies Record<string, PromptSegment>

export type SegmentName = keyof typeof segments

