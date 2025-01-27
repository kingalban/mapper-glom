# mapper-glom

`mapper-glom` is a Singer mapper for GlomMapper.

Built with the [Meltano Mapper SDK](https://sdk.meltano.com) for Singer Mappers.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install mapper-glom
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/mapper-glom.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the mapper.

This section can be created by copy-pasting the CLI output from:

```
mapper-glom --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
mapper is available by running:

```bash
mapper-glom --about
```

### Configure using environment variables

This Singer mapper will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your mapper requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `mapper-glom` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Mapper Directly

```bash
mapper-glom --version
mapper-glom --help
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `mapper-glom` CLI interface directly using `poetry run`:

```bash
poetry run mapper-glom --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This mapper will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd mapper-glom
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Run a test `run` pipeline:
meltano run tap-smoke-test mapper-glom target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps, targets, and mappers.
