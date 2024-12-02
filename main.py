import polars as pl
from datetime import datetime


def main():
    df = pl.DataFrame(
        {
            "RawDatetime": pl.datetime_range(
                start=datetime(2050, 1, 1, 0, 0),
                end=datetime(2050, 1, 1, 1, 0),
                interval="30m",
                closed="both",
                eager=True,
            ).cast(pl.String()),
            "GensetkWh": [1, 2, 3],
            "Emissions": [1, 0, 1],
        }
    )

    # parse datetime strings
    dt_str = (
        df.get_column("RawDatetime")
        # 2020-01-01 00:00 -> 2020-01-01T00:00
        .str.replace(r"^(\d{4}-\d{2}-\d{2}) ", "${1}T")
        # +0000 -> +00:00
        .str.replace(r"\+(\d{2})(\d{2})$", "+${1}:${2}")
        # Z -> +00:00
        .str.replace(r"Z$", "+00:00")
    )
    dt: pl.Series
    e: Exception
    for fmt in [
        # iso8601
        "%+",
        # or omit tz
        "%Y-%m-%dT%H:%M:%S%.f",
        # or omit seconds
        "%Y-%m-%dT%H:%M%:z",
        # or omit seconds and tz
        "%Y-%m-%dT%H:%M",
    ]:
        try:
            print(fmt)
            dt = dt_str.str.to_datetime(format=fmt, time_unit="ms")
            print("found fmt")
            break
        except pl.exceptions.InvalidOperationError as _e:
            e = _e
            print("skipped format")
            continue


if __name__ == "__main__":
    main()
