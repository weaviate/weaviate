### Thanks for looking into contributing to Weaviate!
Contributing works pretty easy. You can do a pull request or you can commit if you are part of a Weaviate team.

### Code of Conduct
Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms. 
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)


### How we use Gitflow
How we use [Gitflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) and how you can contribute following a few steps.

- The master branch is what is released.
- You can create a feature-branch that is named: feature/YOUR-FEATURE-NAME.
- Your feature branch always has the develop branch as a starting point.
- When you are done you can merge your feature into the develop branch _or_ you can request a merge.
- The master branch is protected.

### Tagging your commit

Always add a refference to your issue to your git commit.

For example: `gh-100: This is the commit message`

AKA: smart commits

### Pull Request

If you create a pull request without smart commits, the pull request will be [squashed into](https://blog.github.com/2016-04-01-squash-your-commits/) one git commit.

### Running Weaviate without database

If you work on Weaviate but not need a database. You can run Weaviate like this: `./cmd/weaviate-server/main.go --scheme=http --port=8080 --host=127.0.0.1 --config="dummy"`

### Contributor License Agreement

Contributions to Weaviate must be accompanied by a Contributor License Agreement. You (or your employer) retain the copyright to your contribution; this simply gives us permission to use and redistribute your contributions as part of Weaviate. Go to [this page](https://www.semi.technology/playbook/contributor-license-agreement.html) to read the current agreement.

The process works as follows:

- You contribute by opening a [pull request](#pull-request).
- If your account has no CLA, a DocuSign link will be added as a comment to the pull request.