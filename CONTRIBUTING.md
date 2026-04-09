# Contributing to llmpipe

Thank you for your interest in contributing. This project is licensed under the [GNU Affero General Public License v3.0](LICENSE). By contributing, you agree that your contributions will be licensed under the same terms.

## Before you start

- Open an issue to discuss larger changes or new features when in doubt.
- For bug fixes, a short issue or PR description with reproduction steps helps review.

## Development

- Use a recent Go toolchain matching `go.mod`.
- Run `go test ./...` and `go vet ./...` before opening a pull request.
- Follow existing package layout, naming, and comment style in the tree.

## Pull requests

- Keep changes focused on a single concern when possible.
- Reference related issues in the PR description.
- Do not commit secrets, `.env` files, or generated artifacts that belong in `.gitignore`.

## Code of conduct

Be respectful and constructive in issues and reviews.
