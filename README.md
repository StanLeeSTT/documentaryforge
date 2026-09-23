# DocumentaryForge

## Evidence-First AI Documentary Factory

Factory ID: `documentaryforge_001`

DocumentaryForge is a reusable AI documentary production system.

The factory is designed around:

1. Prompt analysis
2. Dynamic research planning
3. Source discovery
4. Visual discovery
5. Source normalization
6. Independence analysis
7. Question-aware source ranking
8. Targeted retrieval
9. Evidence extraction
10. Atomic claim extraction
11. Claim corroboration
12. Uncertainty and conflict analysis
13. Research gap detection
14. Verified claim ledger
15. Script generation
16. Scene and visual planning
17. Media acquisition
18. Narration
19. Subtitles
20. FFmpeg production
21. Quality control

### Core principle

Source → Evidence → Atomic Claim → Corroboration → Verified Claim → Script → Scene → Media → Final Documentary

### Test documentary

The Indus Valley documentary is an integration test case only.

It is not the factory identity.

### Storage policy

Large media files are treated as runtime/cache artifacts and are not committed to Git.

Downloaded videos have a default seven-day retention policy, subject to:

- downloaded-state verification
- age verification
- protection checks
- final-output/reference checks
- dry-run cleanup
- cleanup logging

### Build rule

A phase is not considered complete merely because tests execute.

The implementation must perform the real operation represented by the phase.
