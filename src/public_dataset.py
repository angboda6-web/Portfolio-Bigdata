from __future__ import annotations

import shutil
from pathlib import Path
from urllib.request import urlopen

DEFAULT_SUPERSTORE_URL = "https://gist.githubusercontent.com/JPJeanlis/98192ecb788a3e5d023618e1ba3ce801/raw/superstore.csv"


def download_public_dataset(output_dir: Path, source_url: str = DEFAULT_SUPERSTORE_URL) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "superstore_raw.tsv"
    if raw_path.exists():
        return raw_path

    with urlopen(source_url) as response, raw_path.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    return raw_path

