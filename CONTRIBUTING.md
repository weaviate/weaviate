## Thanks for looking into contributing to Weaviate!
We really appreciate that you are willing to spend some time and effort to make Weaviate better for everyone! 

We have a detailed [contributor guide](https://weaviate.io/developers/contributor-guide/current/) as a part of our documentation. If you are new, we recommend reading through the [getting started guide for contributors](https://weaviate.io/developers/contributor-guide/current/getting-started/index.html) after reading this overview.

## Finding a good issue to get started
We use the `good-first-issue` labels on issues that we think are great to get started. Typically, they are issues that are isolated to a specific area of Weaviate, e.g. only the API layer or only a specific package.

## What do you need to contribute
To contribute to Weaviate Core you should have at least basic Golang skills. It is also tremendously helpful if you have successfully used Weaviate before, so that you have a rough understanding of the effects of changes etc. Any PR must include tests. We understand that not everyone is very familiar with testing strategies, and how we have setup our tests. This [guide on testing](https://weaviate.io/developers/contributor-guide/current/weaviate-core/tests.html) should be a great start. If you have completed a change, but are struggling with the test, you can also open a Draft PR and ask someone from our team for help. 

## If in doubt, ask.
The Weaviate team consists of some of the nicest people on this planet. If something is unclear or you'd like a second opinion, please don't hesitate to ask. We are glad that you want to help us, so naturally we will also do our best to help you on this journey

## Development Environment
*Please note that the Weaviate team uses Linux and Mac (darwin/arm64) machines exclusively. Development on Windows may lead to unexpected issues.*

Here are some guides to get started in no time:
* [Development Setup](https://weaviate.io/developers/contributor-guide/current/weaviate-core/setup.html)
* [Code Structure](https://weaviate.io/developers/contributor-guide/current/weaviate-core/structure.html)
* [CI/CD](https://weaviate.io/developers/contributor-guide/current/weaviate-core/cicd.html)
* [Testing Philosophy](https://weaviate.io/developers/contributor-guide/current/weaviate-core/tests.html)

## Tagging your commit
Please tag your commit(s) with the appropriate GH issue that your change refers to, e.g. `gh-9001 reduce memory allocations of ACME widget`. Please also include something in your PR description to indicate which issue it will close, e.g. `fixes #9001` or `closes #9001`.

## Pull Request
If you open an external pull request our CI pipeline will get started. This external run will not have access to secrets. This prevents people from submitting a malicious PR to steal secrets. As a result the CI run will be slightly different from an internal one. For example, it will not automatically push a Docker image. If your PR is merged, a container with your changes will be built from the trunk. 

## Agreements 

### Code of Conduct
Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms. 
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)

### Contributor License Agreement

Contributions to Weaviate must be accompanied by a Contributor License Agreement. You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of Weaviate. Go to [this page](https://www.semi.technology/playbooks/misc/contributor-license-agreement.html) to read the current agreement.

The process works as follows:

- You contribute by opening a [pull request](#pull-request).
- If you have not contributed before, our bot will ask to agree with the CLA
