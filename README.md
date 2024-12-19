# Delta utils

## Requirements

- Python 3.10
- Databricks Runtime Version 13.3 and above


## Installation

Look for the latest version in the [Github Tags section](https://github.com/husqvarnagroup/delta_utils/tags) of this repo.
```bash
pip install https://github.com/husqvarnagroup/delta_utils/archive/refs/tags/v0.5.0.zip
```

### Installation on a cluster in Databricks

- Go to your cluster in Databricks
- Go to Libraries
- Click Install new
- Select PyPI
- As the package, enter https://github.com/husqvarnagroup/delta_utils/archive/refs/tags/v0.5.0.zip"
- Press Install

## Development

For package management we use poetry. Go to root of project then enter

``` bash
# Install all packages
poetry install

# Go into poetry shell to get your own virtual enviroment
poetry shell
```

### Mkdocs

To start the documentation server locally enter

```bash
mkdocs serve
```
