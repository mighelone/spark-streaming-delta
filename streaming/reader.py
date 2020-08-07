from typing import Dict, List, Any, Optional, Iterator
import pandas as pd
from pathlib import Path

def read_delta(path: str, filters: Optional[Dict[str, List[Any]]]=None)->pd.DataFrame:
    """[summary]

    Parameters
    ----------
    path : str
        Path of the delta table
    filters : Dict[str, List[Any]]
        Filter for reading the partitions

    Returns
    -------
    pd.DataFrame
        read dataframe
    """

    path = Path(path)

    # res = next(path.glob("**/*.parquet"))

    # partitions = [p.split("=")[0] for p in res.parts[1:-1]]

    manifest_path: Path = path / "_symlink_format_manifest"

    files = [
        f
        for manifest in manifest_path.glob("**/manifest")
        for f in _read_manifest(manifest)
    ]
    return files



def _read_manifest(manifest: Path)->Iterator[Path]:
    """
    [summary]

    [extended_summary]

    Args:
        manifest (Path): [description]

    Yields:
        Iterator[Path]: [description]
    """

    with manifest.open("r") as f:
        for line in f:
            yield Path(line.strip().replace("file:", ""))
