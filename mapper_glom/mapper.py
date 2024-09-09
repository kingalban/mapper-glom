"""GlomMapper mapper class."""

from __future__ import annotations

import typing as t
from functools import cached_property, partial
from typing import TYPE_CHECKING

import glom
import singer_sdk.typing as th
from singer_sdk import _singerlib as singer
from singer_sdk.mapper_base import InlineMapper

if TYPE_CHECKING:
    import logging
    from pathlib import PurePath

TransformerSig = t.Callable[[dict[str, t.Any]], t.Any]

CONFIG_KEY_MARKER = "__config_key__"
PARTIAL_ARGS_MARKER = "__partial_args__"


def config_key(
    key_name: str, partial_args: list[str] | None = None
) -> t.Callable[[t.Any], t.Any]:
    """Annotate a function with its config key and args for the partial."""

    def wrapper(func: t.Callable) -> t.Callable:
        setattr(func, CONFIG_KEY_MARKER, key_name)
        setattr(func, PARTIAL_ARGS_MARKER, partial_args or [])
        return func

    return wrapper


def get_config(func: t.Callable) -> tuple[str | None, list[str] | list[None]]:
    """Get the annotated config key and additional args for the partial."""
    return (
        getattr(func, CONFIG_KEY_MARKER, None),
        getattr(func, PARTIAL_ARGS_MARKER, []),
    )


class Transform:
    """Collect and prepare transforming functions as per configuration."""

    def __init__(self, config: dict, logger: logging.Logger) -> None:
        """Init the logger with the 'transformers'{...} config."""
        self.config = config
        self.logger = logger

    @cached_property
    def _transformers(self) -> dict[str, partial[dict[str, t.Any]]]:
        """A mapping of stream names to transforming functions."""
        transformers = {}
        config_markers = self.__class__.config_markers()

        for stream, config in self.config.get("transformations", {}).items():
            marker, glom_spec = next(iter(config.items()))
            if marker in config_markers:
                func = getattr(self, config_markers[marker])
                ready_func = partial(func, path=glom_spec)
                transformers[stream] = ready_func
                msg = (
                    f"registered transformer for {stream=}:"
                    f" {config_markers[marker]}(_, {glom_spec!r})"
                )
                self.logger.debug(msg)

        return transformers

    @classmethod
    def config_markers(cls) -> dict[str, str]:
        """A mapping of config keys to method names."""
        _keys = {}
        for method_name, method in cls.__dict__.items():
            key_name, _ = get_config(method)
            if key_name is not None:
                _keys[key_name] = method_name
        return _keys

    def transform(self, record_message: dict[str, t.Any]) -> singer.RecordMessage:
        """Transform the record message according to its stream name and the config."""
        stream_name = record_message["stream"]

        if stream_name in self._transformers:
            record_message["record"] = self._transformers[stream_name](
                record_message["record"]
            )
            msg = f"transforming stream {stream_name}"
            self.logger.debug(msg)
        return singer.RecordMessage.from_dict(record_message)

    @config_key("__glom_del__")
    def _del(
        self, record: dict[str, t.Any], path: str, *, ignore_missing: bool = True
    ) -> dict[str, t.Any]:
        """Delete an item from a record.

        Note: the record is mutated!
        """
        msg = f"deleting attribute at {path!r}"
        self.logger.debug(msg)
        return glom.delete(record, path, ignore_missing=ignore_missing)

    @config_key("__glom_assign__")
    def _assign(
        self,
        record: dict[str, t.Any],  # noqa: ARG002
        to_path: str,
        from_path: str,
    ) -> dict[str, t.Any]:
        raise NotImplementedError
        msg = f"setting attribute at {to_path!r} from {from_path!r}"
        self.logger.debug(msg)


class GlomMapper(InlineMapper):
    """Sample mapper for Glom."""

    name = "mapper-glom"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "transformations",
            th.ObjectType(
                additional_properties=th.CustomType(
                    {
                        "type": ["object"],
                        "patternProperties": {
                            "type": "object",
                            "oneOf": [
                                {
                                    "type": "object",
                                    marker: {"type": "string"},
                                    "required": marker,
                                    "additionalProperties": False,
                                }
                                for marker in Transform.config_markers()
                            ],
                        },
                    },
                ),
            ),
            required=True,
            description=(
                "A mapping of stream names to transformation definitions.\n"
                'eg: {"transformations": {"stream1": {"__glom_del__": "col1.prop1"}}}'
            ),
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        self.transformer = Transform(self._config, self.logger)

    def map_schema_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.

        TODO: inspect the schema and modify it accordingly.
        """
        yield singer.SchemaMessage.from_dict(message_dict)

    def map_record_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[singer.RecordMessage]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        yield self.transformer.transform(message_dict)

    def map_state_message(self, message_dict: dict) -> t.Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        yield singer.StateMessage.from_dict(message_dict)

    def map_activate_version_message(
        self, message_dict: dict
    ) -> t.Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        yield singer.ActivateVersionMessage.from_dict(message_dict)


if __name__ == "__main__":
    GlomMapper.cli()
