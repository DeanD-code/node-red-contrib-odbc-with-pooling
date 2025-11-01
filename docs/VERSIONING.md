# Version Management

This project uses automatic semantic versioning based on commit messages.

## How It Works

The GitHub Actions workflow automatically detects version bump types from commit messages using **Conventional Commits** patterns:

### Version Bump Types

| Commit Pattern | Version Bump | Example |
|---------------|--------------|---------|
| `BREAKING:` or `major:` | **MAJOR** (1.0.0 → 2.0.0) | Breaking API changes |
| `feat:` or `feature:` or `add:` | **MINOR** (1.0.0 → 1.1.0) | New features |
| `fix:` or `bugfix:` or `patch:` or `bug:` | **PATCH** (1.0.0 → 1.0.1) | Bug fixes |
| No pattern match | **Build metadata** | Adds timestamp + SHA |

## Commit Message Examples

### Major Version Bump (Breaking Changes)
```
BREAKING: Change API structure
BREAKING CHANGE: Removed deprecated methods
major: Refactor entire module
```

### Minor Version Bump (New Features)
```
feat: Add new ODBC connection pooling
feature: Support for MySQL
add: New query node type
```

### Patch Version Bump (Bug Fixes)
```
fix: Resolve connection timeout issue
bugfix: Fix parameter binding error
patch: Correct status indicator
bug: Handle null values properly
```

### Build Metadata (No Pattern Match)
```
docs: Update README
chore: Update dependencies
refactor: Clean up code
```

For commits without conventional commit patterns, the version will add build metadata like: `1.0.0+build.20241101183606.abc1234`

## Current Version

The current version is stored in `package.json`. Each publish will:
1. Analyze the last commit message
2. Determine the version bump type
3. Increment the version accordingly
4. Add build metadata (timestamp + commit SHA)
5. Publish to npm

## Manual Version Override

To manually set a version:
1. Edit `package.json` and change the `version` field
2. Commit with message like: `chore: bump version to 2.0.0`
3. Push to trigger the workflow

## Release Tags

You can also create git tags for releases:
```bash
git tag v1.0.0
git push origin v1.0.0
```

The workflow will respect version tags and use them instead of auto-incrementing.

