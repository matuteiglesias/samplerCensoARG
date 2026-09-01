"""Consumer binding from a governed target-population parent into the sampler."""
from __future__ import annotations

import hashlib
from pathlib import Path

from .target_population import validate_target_population_parent
from .target_year import build_target_year_release


def build_target_year_release_from_parent(
    databasepath: Path,
    output_root: Path,
    *,
    target_population_parent: Path,
    target_year: int,
    fraction: float = 0.01,
    seed: int = 20260831,
    geography_path: Path | None = None,
    max_households: int = 100000,
) -> Path:
    """Build a sample using the exact identity declared by a governed parent.

    The underlying sampler already records both the supplied parent release ID
    and the SHA-256 of ``target_population.csv``. This wrapper removes the
    possibility of hand-typing a source ID that does not correspond to the
    bytes actually consumed.
    """
    parent = Path(target_population_parent).expanduser().resolve()
    manifest = validate_target_population_parent(parent)
    manifest_path = parent / "manifest.json"
    manifest_sha256 = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    release_id = manifest.get("release_id")
    if not isinstance(release_id, str) or not release_id:
        raise ValueError("governed_target_population_release_id_missing")

    source_id = f"{release_id}@manifest-sha256:{manifest_sha256}"
    return build_target_year_release(
        databasepath,
        output_root,
        target_population_path=parent / "target_population.csv",
        target_source_id=source_id,
        target_year=target_year,
        fraction=fraction,
        seed=seed,
        geography_path=geography_path,
        max_households=max_households,
    )
