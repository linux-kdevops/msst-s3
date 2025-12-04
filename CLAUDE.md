# DCO
For commits use the Developer Certificate of Origin:

Signed-off-by

# Generated-by

Use:

Generated-by: Claude AI
Signed-off-by: Luis Chamberlain <mcgrof@kernel.org>

If another AI is used, use an appropriate tag for the AI.

Do not use:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
Co-Authored-By: Claude <noreply@anthropic.com>
```

These do not add value. As short tag is better as noted erlier with the
Generated-by tag once.

# Black

Black should be used on all python scripts.

# Docs

Keep max column length to 80 characters.

# Avoid Shopping Cart Lists

**CRITICAL RULE: NEVER USE BULLET POINTS OR ITEMIZED LISTS IN COMMIT MESSAGES**

Generative AI seems to like to make commit logs long itemized lists of things
it did. This is stupid. This should be avoided. It is creating very silly
commit logs. Use plain English and get to the point. Be as clear as possible
and get to the point of not what you want to communicate, but rather what
will make a reviewer easily understand what the heck you are implementing.

You should *think* hard about your commit log, always.

**WRONG - Shopping cart list with bullet points:**
```
Refactored to separate concerns:
- Distribution files handle package installation and set nfs_server_service
  variable (nfs-kernel-server for Debian/Ubuntu, nfs-server for RedHat/Fedora)
- Single systemd task in main.yml handles service enablement using the variable
```

**WRONG - Change list:**
```
Fix by changing:
  - mirror_service_status.item → mirror_service_status.results
  - mirror_timer_status.item → mirror_timer_status.results
```

**Correct - Plain English:**
```
Each distribution file now handles package installation and sets the
nfs_server_service variable to the appropriate service name for that
distribution. A single systemd task in main.yml then handles service
enablement using the variable.
```

**Correct - Plain English:**
```
Change both debug tasks to iterate over the .results list instead of
the non-existent .item attribute.
```

# Test Creation Rules - CRITICAL

## NEVER Auto-Generate Tests
- NEVER create test generation scripts
- NEVER generate tests programmatically with loops and incrementing numbers
- NEVER create hundreds of nearly identical tests with slight parameter variations
- Each test file must be carefully hand-crafted with a specific purpose

## Test Quality Requirements
- Every test MUST test actual S3 API functionality or edge cases
- Tests must NOT just store different types of fake business data
- Categories like "compliance", "healthcare", "financial", etc. are NOT S3 features
- Test names should reflect what S3 feature they test, not business domains

## What Makes a Good Test
- Tests a specific S3 API operation or feature (e.g., multipart upload, versioning, ACLs)
- Tests edge cases and error conditions (e.g., special characters, size limits)
- Tests integration between S3 features (e.g., versioning with lifecycle policies)
- Has clear assertions and validates actual behavior
- Provides value in detecting S3 compatibility issues

## What Makes a Bad Test
- Just stores fake business data (trading records, medical records, etc.)
- Parameter variations that should be a single parameterized test
- Tests that claim to test "edge cases" but just upload a file
- Tests that claim to test "stress" but only upload a few small files
- Any test that doesn't actually test S3 functionality

## Test Organization
- Group tests by S3 feature, not by business domain
- Use parameterized tests instead of multiple files for parameter variations
- Quality over quantity: 30 good tests > 3000 meaningless tests
- Each test should have a clear, documented purpose

## Remember
Creating thousands of auto-generated tests that don't actually test S3 features is:
- A waste of repository space
- A maintenance nightmare
- Provides zero value for S3 compatibility testing
- Makes the project look amateurish and poorly designed

THINK before creating any test: "Does this test an actual S3 feature or just store fake data?"

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
