# Delta utils

## Installation

```bash
pip install git+https://github.com/husqvarnagroup/delta_utils.git
```

### Installation on a cluster in Databricks

- Go to your cluster in Databricks
- Go to Libraries
- Click Install new
- Select PyPI
- As the package, enter "git+https://github.com/husqvarnagroup/delta_utils.git"
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
