class ConfigValidator:
    """Validates dataset ingestion configuration."""

    REQUIRED_SECTIONS = {
        "source",
        "target",
        "schema",
        "write",
        "audit",
    }

    def validate(self, config: dict) -> None:
        """Validate the overall configuration structure."""

        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary.")

        if "dataset_name" not in config:
            raise ValueError("Missing required field: dataset_name")

        missing_sections = self.REQUIRED_SECTIONS - config.keys()

        if missing_sections:
            raise ValueError(
                f"Missing required sections: {sorted(missing_sections)}"
            )


            