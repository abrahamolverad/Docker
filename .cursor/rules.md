# Cursor Rules for RokAi

- Read AGENTS.md first. Obey constraints, risk controls, and environment usage.
- Never hardcode secrets. Use env vars only.
- Maintain backward compatibility with CLI flags and config files.
- Add/extend tests for every bug fix or feature. Do not skip tests.
- Prefer small, atomic PRs with clear titles and descriptions.
- Keep logs structured (JSON if possible) and respect LOG_LEVEL.
- Performance matters: avoid O(N^2) scans on full universes; batch API calls.
