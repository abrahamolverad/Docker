# Agents Manual for RokAi

This repository includes AI agents that perform code modifications, trading execution, and automation. All agents must follow these rules:

## Environment & Secrets

- **Never** hardcode API keys or secrets in source code. Use environment variables exclusively.
- Support local `.env` files for development, and rely on Render or CI env vars in production.

## Risk Management

- Enforce position size caps, ATR-based stop-loss and take-profit levels, maximum concurrent positions, and circuit breakers.
- Validate pre-market and post-market trading permissions.
- All order placements must be idempotent and handle retries safely.

## Development Practices

- Maintain backward compatibility for command-line flags and configuration files.
- Every bug fix or new feature must include or extend unit tests.
- Keep pull requests small and focused with clear titles and descriptions.
- Follow the coding standards enforced by Ruff and Mypy; lint and type-check before merging.

## Observability

- Use structured logging (JSON when possible) and respect the `LOG_LEVEL` environment variable.
- Add health-check endpoints and metrics where appropriate to monitor service health.

By adhering to these guidelines, agents will help ensure the stability, safety, and maintainability of the RokAi codebase.
