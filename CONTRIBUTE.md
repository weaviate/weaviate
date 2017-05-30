### Thanks for looking into contributing to Weaviate!
Contributing works pretty easy. You can do a pull request or you can commit if you are part of a Weaviate team.

### How we use Gitflow
How we use [Gitflow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) and how you can contribute following a few steps.

- The master branch is what is released.
- You can fork the weaviate project and create a feature branch that is named: feature/YOUR-FEATURE-NAME.
- Your feature branch always has the develop branch as a starting point.
- To keep up to date with the weaviate/weaviate develop branch, setup the weaviate/weaviate as a [remote tracking branch](https://help.github.com/articles/configuring-a-remote-for-a-fork/).
- When you are done you can request a merge, via a pull request to develop. Make sure your work is rebased on top of develop so no merge commits can occur. If develop is updated before the PR can be merged, you may be asked to rebase again.
- The master branch is protected.

### Tagging your commit

Always add a refference to your issue to your git commit.

For example: `gh-100: This is the commit message`

AKA: smart commits
