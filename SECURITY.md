# Security policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately — do **not** open a
public GitHub issue or discuss them in public threads.

Use GitHub's private vulnerability reporting on this repository
(Security → Report a vulnerability), or reach the maintainers through
the contact on the repository profile.

Include: the affected version/commit, a description of the impact,
reproduction steps or a proof of concept, and whether you are willing to
be credited in a fix announcement.

We aim to acknowledge reports within a few days and to coordinate a fix
and disclosure timeline with the reporter.

## Scope notes

- TesseraDB stores tenant databases in the operator's own AWS account.
  Issues that require prior AWS credentials for the victim's account are
  generally out of scope for a vulnerability report, but fencing,
  integrity or isolation breaks that cross tenant boundaries are in
  scope.
- The `tessera.experimental` surface is explicitly unstable and opt-in;
  report issues there too, but expect fixes on the same relaxed cadence
  as the surface itself.

## Supported versions

Security fixes land on the latest release candidate or stable line; see
`CHANGELOG.md` and `release/gates.json` for the current stage.
