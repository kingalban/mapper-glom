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
    from pathlib import PurePath


def glom_del(record_dict: dict[str, t.Any], config: dict[str, str]) -> dict[str, t.Any]:
    """Delete an item from a dict with Glom syntax.

    Note that this MUTATES the original dict.
    """
    return glom.delete(record_dict, config, ignore_missing=True)


def glom_assign(
    record_dict: dict[str, t.Any],  # noqa: ARG001
    config: dict[str, str],  # noqa: ARG001
) -> dict[str, t.Any]:
    """Sets a value in a dict with Glom syntax.

    not implemented.
    """
    raise NotImplementedError


def glom_get(record_dict: dict[str, t.Any], config: dict[str, str]) -> dict[str, t.Any]:  # noqa: ARG001
    """Retrievs an item from a dict with Glom syntax.

    not implemented
    """
    raise NotImplementedError


GLOM_DELETE_MARKER = "__glom_del__"
GLOM_ASSIGN_MARKER = "__glom_assign__"
GLOM_GET_MARKER = "__glom_get__"
MARKERS = {
    GLOM_DELETE_MARKER: glom_del,
    GLOM_ASSIGN_MARKER: glom_assign,
    GLOM_GET_MARKER: glom_get,
}


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
                                for marker in MARKERS
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

    @cached_property
    def mutators(self) -> t.Mapping[str, t.Callable]:
        """A mapping of stream names to mutating functions.

        Note that these are not transformations, the record will be *mutated*
        """
        mutators = {}
        for stream, config in self.config.get("transformations").items():
            marker, glom_spec = next(iter(config.items()))
            func = MARKERS[marker]
            ready_func = partial(func, config=glom_spec)
            mutators[stream] = ready_func
        return mutators

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
        stream_name: str = message_dict["stream"]
        if stream_name in self.mutators:
            _ = self.mutators[stream_name](message_dict.get("record"))
            yield singer.RecordMessage.from_dict(message_dict)

        else:
            yield singer.RecordMessage.from_dict(message_dict)

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
