import dataclasses
import pathlib
import yaml

from typing import Optional


@dataclasses.dataclass
class CliConfig:
    recurrence_distance: Optional[int]
    recurrence_size: Optional[int]
    chunk_size: Optional[int]
    consistency_sample_size: Optional[int]
    usable_memory: Optional[int]

    def to_yaml(self, file_path: pathlib.Path) -> None:
        """Stores data from fields into yaml file at file_path"""
        try:
            with file_path.open() as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            file_path.touch()
            config_data = {}

        for field in dataclasses.fields(type(self)):
            config_data[field.name] = getattr(self, field.name)
        with file_path.open(mode="w") as f:
            yaml.safe_dump(config_data, f)

    @classmethod
    def from_yaml(cls, file_path: pathlib.Path) -> "CliConfig":
        """Creates CliConfig object with data from yaml file at file_path"""
        try:
            with file_path.open() as f:
                config_data = yaml.safe_load(f)
        except FileNotFoundError:
            config_data = {}

        cli_config_data = {
            field.name: config_data.get(field.name)
            for field in dataclasses.fields(cls)
        }
        return cls(**cli_config_data)

    def update_config(
        self,
        recurrence_size: int,
        recurrence_distance: int,
        chunk_size: int,
        consistency_sample_size: int,
        usable_memory: int,
    ) -> None:
        """Updates fields."""
        self.recurrence_size = recurrence_size
        self.recurrence_distance = recurrence_distance
        self.chunk_size = chunk_size
        self.consistency_sample_size = consistency_sample_size
        self.usable_memory = usable_memory
