"""Bridge into the current `features_v4` implementation."""

from __future__ import annotations

from pathlib import Path

from autobot.features import (
    FeatureBuildV4Options,
    FeatureBuildV4Summary,
    FeatureValidateV4Options,
    FeatureValidateV4Summary,
    FeaturesV4Config,
    build_features_dataset_v4,
    load_features_v4_config,
    validate_features_dataset_v4,
)


def load_v4_features_config(config_dir: Path, *, base_config: dict | None = None, filename: str = "features_v4.yaml") -> FeaturesV4Config:
    return load_features_v4_config(config_dir, base_config=base_config, filename=filename)


def build_v4_features_dataset(config: FeaturesV4Config, options: FeatureBuildV4Options) -> FeatureBuildV4Summary:
    return build_features_dataset_v4(config, options)


def validate_v4_features_dataset(config: FeaturesV4Config, options: FeatureValidateV4Options) -> FeatureValidateV4Summary:
    return validate_features_dataset_v4(config, options)

