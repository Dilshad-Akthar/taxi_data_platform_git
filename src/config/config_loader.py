from pathlib import Path
import yaml


class ConfigLoader:
    """Loads dataset configuration from YAML files."""

    def load(self, config_path: str) -> dict:
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        with path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        if config is None:
            raise ValueError(
                f"Configuration file is empty: {config_path}"
            )

        return config
