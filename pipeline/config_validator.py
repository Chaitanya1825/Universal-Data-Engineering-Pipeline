"""
Centralized Configuration Validation Module (Stage 11).

Validates all pipeline configuration files:
- config/pipeline.json
- config/transformations.json
- config/validation.json
- config/database.json
- config/incremental.json
- config/analytics.json
- config/powerbi.json

Ensures:
- Valid JSON syntax
- Required configuration keys
- Correct types and value domains
- Cross-configuration consistency
- Domain-agnostic schema compliance
"""

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any, Optional

ALLOWED_STAGE_NAMES = [
    "foundation",
    "ingestion",
    "incremental",
    "profiling",
    "transformation",
    "validation",
    "database",
    "analytics",
    "powerbi",
]


@dataclass
class ConfigValidationIssue:
    """Diagnostic detail for a configuration error or warning."""
    config_file: str
    field_name: str
    severity: str  # "ERROR" or "WARNING"
    message: str


@dataclass
class ConfigValidationResult:
    """Consolidated outcome of all configuration validations."""
    is_valid: bool
    issues: list[ConfigValidationIssue] = field(default_factory=list)

    @property
    def errors(self) -> list[ConfigValidationIssue]:
        return [i for i in self.issues if i.severity == "ERROR"]

    @property
    def warnings(self) -> list[ConfigValidationIssue]:
        return [i for i in self.issues if i.severity == "WARNING"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "issues": [asdict(i) for i in self.issues],
        }


class ConfigurationValidator:
    """
    Validates syntax, structure, value ranges, and types across all
    JSON configuration files in the project.
    """

    def __init__(self, config_dir: Optional[Path | str] = None):
        self.config_dir = Path(config_dir) if config_dir else Path(__file__).resolve().parent.parent / "config"

    def _add_error(self, issues: list[ConfigValidationIssue], config_name: str, field_name: str, msg: str) -> None:
        issues.append(ConfigValidationIssue(config_name, field_name, "ERROR", msg))

    def _add_warning(self, issues: list[ConfigValidationIssue], config_name: str, field_name: str, msg: str) -> None:
        issues.append(ConfigValidationIssue(config_name, field_name, "WARNING", msg))

    def validate_pipeline_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "pipeline.json"
        enabled = data.get("enabled_stages")
        if enabled is None:
            self._add_error(issues, cfg_name, "enabled_stages", "Missing required 'enabled_stages' key.")
        elif not isinstance(enabled, list):
            self._add_error(issues, cfg_name, "enabled_stages", "'enabled_stages' must be a list of strings.")
        else:
            invalid = [s for s in enabled if s not in ALLOWED_STAGE_NAMES]
            if invalid:
                self._add_error(issues, cfg_name, "enabled_stages", f"Invalid stage names: {invalid}.")

        for flag in ["fail_fast", "continue_on_database_unavailable", "generate_reports", "logging_enabled"]:
            if flag in data and not isinstance(data[flag], bool):
                self._add_warning(issues, cfg_name, flag, f"'{flag}' should be a boolean.")

    def validate_transformations_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "transformations.json"
        if "string_normalization" in data:
            val = data["string_normalization"]
            if val not in ["none", "lower", "upper", "title"]:
                self._add_error(issues, cfg_name, "string_normalization", f"Invalid option '{val}'. Allowed: none, lower, upper, title.")
        if "trim_whitespace" in data and not isinstance(data["trim_whitespace"], bool):
            self._add_warning(issues, cfg_name, "trim_whitespace", "'trim_whitespace' should be boolean.")

    def validate_validation_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "validation.json"
        if "rules" in data and not isinstance(data["rules"], dict):
            self._add_error(issues, cfg_name, "rules", "'rules' must be a dictionary.")

    def validate_database_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "database.json"
        if "batch_size" in data:
            bs = data["batch_size"]
            if not isinstance(bs, int) or bs <= 0:
                self._add_error(issues, cfg_name, "batch_size", "'batch_size' must be a positive integer.")
        if "if_exists" in data:
            val = data["if_exists"]
            if val not in ["append", "replace", "fail"]:
                self._add_error(issues, cfg_name, "if_exists", f"Invalid if_exists '{val}'. Allowed: append, replace, fail.")

    def validate_incremental_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "incremental.json"
        if "hash_algorithm" in data:
            alg = str(data["hash_algorithm"]).lower()
            if alg not in ["sha256", "md5"]:
                self._add_error(issues, cfg_name, "hash_algorithm", f"Unsupported hash algorithm '{alg}'. Allowed: sha256, md5.")
        if "chunk_size" in data:
            cs = data["chunk_size"]
            if not isinstance(cs, int) or cs <= 0:
                self._add_error(issues, cfg_name, "chunk_size", "'chunk_size' must be a positive integer.")

    def validate_analytics_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "analytics.json"
        d_lim = data.get("default_limit", 1000)
        m_lim = data.get("max_limit", 10000)
        if not isinstance(d_lim, int) or d_lim <= 0:
            self._add_error(issues, cfg_name, "default_limit", "'default_limit' must be a positive integer.")
        if not isinstance(m_lim, int) or m_lim <= 0:
            self._add_error(issues, cfg_name, "max_limit", "'max_limit' must be a positive integer.")
        if isinstance(d_lim, int) and isinstance(m_lim, int) and d_lim > m_lim:
            self._add_error(issues, cfg_name, "default_limit", "'default_limit' cannot exceed 'max_limit'.")

    def validate_powerbi_config(self, data: dict[str, Any], issues: list[ConfigValidationIssue]) -> None:
        cfg_name = "powerbi.json"
        modes = data.get("supported_connection_modes", ["import", "directquery"])
        mode = data.get("connection_mode", "import")
        if mode not in modes:
            self._add_error(issues, cfg_name, "connection_mode", f"Mode '{mode}' not in supported_connection_modes {modes}.")
        if "max_tables" in data:
            mt = data["max_tables"]
            if not isinstance(mt, int) or mt <= 0:
                self._add_error(issues, cfg_name, "max_tables", "'max_tables' must be a positive integer.")

    def validate_all(self) -> ConfigValidationResult:
        """Validate all configuration files in the configured directory."""
        issues: list[ConfigValidationIssue] = []
        validators = {
            "pipeline.json": self.validate_pipeline_config,
            "transformations.json": self.validate_transformations_config,
            "validation.json": self.validate_validation_config,
            "database.json": self.validate_database_config,
            "incremental.json": self.validate_incremental_config,
            "analytics.json": self.validate_analytics_config,
            "powerbi.json": self.validate_powerbi_config,
        }

        for fname, validator_func in validators.items():
            fpath = self.config_dir / fname
            if not fpath.exists():
                self._add_error(issues, fname, "file", f"Configuration file '{fname}' not found.")
                continue

            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    self._add_error(issues, fname, "json", "Root JSON element must be a dictionary object.")
                    continue
                validator_func(data, issues)
            except json.JSONDecodeError as e:
                self._add_error(issues, fname, "json_syntax", f"Malformed JSON: {e}")
            except Exception as e:
                self._add_error(issues, fname, "general", f"Error reading config: {e}")

        is_valid = len([i for i in issues if i.severity == "ERROR"]) == 0
        return ConfigValidationResult(is_valid=is_valid, issues=issues)
