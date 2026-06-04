# Security Policy

## Supported Versions

We release security updates for the latest stable version. Please ensure you are running the most recent release before reporting a vulnerability.

| Version | Supported          |
| ------- | ------------------ |
| latest  | ✅ |
| < latest| ❌ |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Preferred method:** Use [GitHub Security Advisories](https://github.com/tonyabasy/flink-sql-bootstrap/security/advisories/new) to submit a private vulnerability report. This ensures the issue is handled confidentially and tracked properly.

**Alternative method:** If you cannot use GitHub Security Advisories, open a new issue and prefix the title with `[SECURITY]`. Please **do not** disclose sensitive details in public issues — provide a high-level summary and we will reach out privately.

## What to Include

To help us triage and fix the issue quickly, please include:

- A clear description of the vulnerability and its impact
- Steps to reproduce (minimal SQL script, CLI args, environment info)
- Affected versions or commit ranges
- Any suggested mitigation or fix (optional)

## Response Timeline

| Phase | Timeline |
|-------|----------|
| Acknowledgment | Within 7 days of receiving the report |
| Initial assessment | Within 14 days |
| Fix & release | Depends on severity; critical issues are prioritized |
| Public disclosure | Coordinated with the reporter after a fix is available |

## Security Best Practices for Users

- Always run the latest release of `flink-sql-bootstrap`.
- Review SQL scripts from untrusted sources before execution — this tool compiles and submits arbitrary SQL to a Flink cluster.
- Ensure your Flink cluster is properly secured (network isolation, authentication enabled) when using this tool in production.
- Report any suspicious behavior or unexpected network requests promptly.

## Acknowledgments

We thank security researchers and community members who responsibly disclose vulnerabilities. Credit will be given in the release notes unless you prefer to remain anonymous.
