# Contributing to stellar-etl

Thanks for taking the time to improve stellar-etl!

The following is a set of guidelines for contributions and may change over
time. Feel free to suggest improvements to this document in a pull request. We
want to make it as easy as possible to contribute changes that help the Stellar
network grow and thrive. There are a few guidelines that we ask contributors to
follow so that we can merge your changes quickly.

Start with the [Stellar Contribution Guide](https://github.com/stellar/.github/blob/master/CONTRIBUTING.md),
which applies to every Stellar project. For instructions on building, running,
and testing the ETL, see [DEVELOPING.md](DEVELOPING.md).

## Table of Contents

- [Branches](#branches)
- [Commit messages](#commit-messages)
- [Pull requests](#pull-requests)
- [Issues](#issues)
- [Releases](#releases)
- [Go style guide](#go-style-guide)
- [Documentation](#documentation)
- [Security](#security)

## Branches

Before creating a branch it is very important to know if your modification to
this repository is a release (breaking changes), a feature (functionalities) or
a patch (to fix bugs). With that information, create your branch name like this:

- `major/<branch-name>`
- `minor/<branch-name>`
- `patch/<branch-name>`

If branch is already made, just rename it _before creating the pull request_.

The prefix is not just a convention: the release workflow reads it to decide
which part of the version to increment when your pull request merges. See
[Releases](#releases).

## Commit messages

- Use the present tense ("Add feature" not "Added feature").
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...").
- Keep the subject line short and put context in the body.

## Pull requests

Open pull requests against `master`, or against the active `release-*` branch if
the change belongs to a release already in flight.

The [pull request template](.github/pull_request_template.md) asks you to fill
in **What**, **Why**, and **Known limitations**. The "Why" matters most — include
enough context that a reviewer who has not seen the issue can follow the change.

- Keep scope narrow. Aim for something a reviewer can get through in about 20
  minutes; break bigger work into a series of pull requests.
- Do not mix refactoring with feature work. Refactoring pull requests touch far
  more code and get reviewed in less detail, so keep them separate.
- Add tests for the most critical parts of new functionality and fixes. If your
  change alters an export's output, regenerate the golden files in `testdata/`
  and call out the diff in the description.
- Update the [README](README.md) or [DEVELOPING.md](DEVELOPING.md) when you add
  a command, change a flag, or change how the repository is built or run.
- Pull requests need to pass linting, the `internal` build, and the integration
  tests, which enforce a 55% coverage floor.

## Issues

- Label issues with `bug` if they are clearly a bug, and `feature request` if
  they are a feature request.
- For a bug, include the command you ran, the flags, the network, and the
  ledger range, along with the error output.

## Releases

Merging a pull request into `master` tags a new version and publishes a GitHub
release automatically. The version bump comes from your **branch prefix**:

| Branch prefix | Version bump | Use for                                         |
| ------------- | ------------ | ----------------------------------------------- |
| `major/`      | major        | Incompatible changes                            |
| `minor/`      | minor        | New functionality, backward compatible          |
| anything else | patch        | Backward-compatible bug fixes and documentation |

Note that a branch with no recognized prefix still produces a patch release, so
a breaking change on a mis-named branch gets an incorrect version. Rename the
branch before opening the pull request.

Merging to `master` also builds a Docker image tagged with the 9-character
commit SHA and pushes it to Google Artifact Registry. Consumers such as
[stellar-etl-airflow](https://github.com/stellar/stellar-etl-airflow) pin that
image tag, so a change here is not live anywhere until the pin is bumped.

## Go style guide

- Format with `gofmt`, or preferably `goimports`. The pre-commit hooks run both.
- Follow [Effective Go](https://go.dev/doc/effective_go) and
  [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments).
- Document exported package elements: vars, consts, funcs, types.
- Log errors through `cmdLogger.LogError` rather than calling `Error` or `Fatal`
  directly, so the `--strict-export` flag is honored. See
  [Logging](DEVELOPING.md#logging).
- Tests are better than no tests.

## Documentation

- Command usage, flags, and output schemas belong in the [README](README.md).
- Build, run, test, and debug instructions belong in
  [DEVELOPING.md](DEVELOPING.md).
- Contribution process, style, and release mechanics belong here.
- Prose in Markdown files is formatted by `prettier` through pre-commit, so run
  the hooks before pushing.

## Security

Please do not open a public issue for a suspected vulnerability. Follow the
[Stellar security policy](https://github.com/stellar/.github/blob/master/SECURITY.md)
instead.
