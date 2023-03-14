# Contribution Process

The following is a set of guidelines for contributing to Firehose. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request. Here are some important resources:

- The [Concepts](../guides/create_firehose.md) section will explain to you about Firehose architecture,
- Our [roadmap](https://github.com/goto/firehose/blob/main/docs/roadmap.md) is the 10k foot view of where we're going, and
- Github [issues](https://github.com/goto/firehose/issues) track the ongoing and reported issues.

Development of Firehose happens in the open on GitHub, and we are grateful to the community for contributing bug fixes and improvements. Read below to learn how you can take part in improving Firehose.

## What to contribute

Your contribution might make it easier for other people to use this product. Better usability often means a bigger user base, which results in more contributors, which in turn can lead to higher-quality software in the long run.

You don’t have to be a developer to make a contribution. We also need technical writers to improve our documentation and designers to make our interface more intuitive and attractive. In fact, We are actively looking for contributors who have these skill sets.

The following parts are open for contribution:

- Adding a new functionality
- Improve an existing functionality
- Adding a new sink
- Improve an existing sink
- Provide suggestions to make the user experience better
- Provide suggestions to Improve the documentation

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/goto/firehose/labels/good%20first%20issue) that contain bugs that have a relatively limited scope. This is a great place to get started.

## How can I contribute?

We use RFCs and GitHub issues to communicate ideas.

- You can report a bug or suggest a feature enhancement or can just ask questions. Reach out on Github discussions for this purpose.
- You are also welcome to add a new common sink in [depot](https://github.com/goto/depot), improve monitoring and logging and improve code quality.
- You can help with documenting new features or improve existing documentation.
- You can also review and accept other contributions if you are a maintainer.

Please submit a PR to the main branch of the Firehose repository once you are ready to submit your contribution. Code submission to Firehose \(including a submission from project maintainers\) requires review and approval from maintainers or code owners. PRs that are submitted by the general public need to pass the build. Once the build is passed community members will help to review the pull request.

## Becoming a maintainer

We are always interested in adding new maintainers. What we look for is a series of contributions, good taste, and an ongoing interest in the project.

- maintainers will have write access to the Firehose repositories.
- There is no strict protocol for becoming a maintainer or PMC member. Candidates for new maintainers are typically people that are active contributors and community members.
- Candidates for new maintainers can also be suggested by current maintainers or PMC members.
- If you would like to become a maintainer, you should start contributing to Firehose in any of the ways mentioned. You might also want to talk to other maintainers and ask for their advice and guidance.

## Guidelines

Please follow these practices for your change to get merged fast and smoothly:

- Contributions can only be accepted if they contain appropriate testing \(Unit and Integration Tests\).
- If you are introducing a completely new feature or making any major changes to an existing one, we recommend starting with an RFC and get consensus on the basic design first.
- Make sure your local build is running with all the tests and checkstyle passing.
- If your change is related to user-facing protocols/configurations, you need to make the corresponding change in the documentation as well.
- Docs live in the code repo under [`docs`](https://github.com/goto/firehose/tree/main/docs/docs/README.md) so that changes to that can be done in the same PR as changes to the code.
