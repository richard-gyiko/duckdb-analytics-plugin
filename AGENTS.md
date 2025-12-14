# Agent Guidelines

## Skill Best Practices

While working on a skill or planning a new spec to extend or modify the skill, make sure to follow the skill best practices [here](docs/claude_skills_best_practices.md)

## Development Workflow

Follow this workflow when creating or modifying skills:

### 1. Create Specs
- Define the feature requirements and scope
- Document expected behavior and edge cases
- **Read [best practices](docs/claude_skills_best_practices.md) before finalizing specs**

### 2. Create Implementation Plan
- Break down the work into actionable steps
- Identify files to create or modify
- **Use Context7 tools to fetch up-to-date library/SDK documentation** for any libraries being used
- **Read [best practices](docs/claude_skills_best_practices.md) and verify the plan aligns with them**

### 3. Cross-Check Specs and Implementation Plan
- Verify the plan covers all spec requirements
- Ensure nothing is missed or over-engineered
- Confirm alignment with best practices (conciseness, progressive disclosure, etc.)

### 4. Implement
- Follow the implementation plan step by step
- Keep changes minimal and focused

### 5. Validate Against Best Practices
- Review implementation against [best practices](docs/claude_skills_best_practices.md)
- Check for: conciseness, appropriate freedom levels, consistent terminology
- Ensure progressive disclosure patterns are used correctly

### 6. Cross-Check Implementation with Plan
- Verify all planned items are implemented
- Confirm no scope creep or missing pieces

### 7. Create Tests
- Write meaningful, valuable tests that cover:
  - Core functionality
  - Edge cases from specs
  - Error handling

### 8. Run Tests and Fix Issues
- Execute all tests
- Fix any failures
- Re-run until all tests pass

## Key Principle

**Always consult best practices during planning phases.** Before finalizing specs or implementation plans, read through [claude_skills_best_practices.md](docs/claude_skills_best_practices.md) to ensure alignment with:
- Conciseness guidelines
- Progressive disclosure patterns
- Workflow and feedback loop recommendations
- Template and examples patterns