"""Quick inspector for a Delta table — no Spark session needed."""
import sys

from deltalake import DeltaTable

from streaming.config import BRONZE_PATH


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else BRONZE_PATH
    table = DeltaTable(path)
    df = table.to_pandas()
    print(f"{path}: {len(df)} rows, version {table.version()}")
    print(df.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
