# objstore-github

[objstore](https://github.com/theduke/objstore) backend for GitHub repositories.

Exposes a GitHub repository as an object store, allowing you to list, and
read/write/delete files over the Github API.

## Testing


```sh
GITHUB_TEST_URI=github://<token>@<owner>/<repo> cargo test
```
