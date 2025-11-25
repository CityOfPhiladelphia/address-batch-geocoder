import chardet
from pathlib import Path


def detect_file_encoding(file_path: str):
    # Attempt to determine the filetype
    # by reading the first 10kb of a file
    with open(file_path, "rb") as f:
        raw_data = f.read(100000)

    result = chardet.detect(raw_data)
    encoding = result["encoding"]

    return encoding


def recode_to_utf8(src_path: str, dst_path: str, src_encoding: str) -> Path:
    """
    Reincode an input file to account for non-standard characters, line by line.
    """

    src = Path(src_path)
    if dst_path is None:
        dst = src.with_suffix(src.suffix + ".utf8")

    else:
        dst = Path(dst_path)

    with (
        src.open("r", encoding=src_encoding, errors="strict", newline="") as fin,
        dst.open("w", encoding="utf-8", newline="") as fout,
    ):
        for line in fin:
            fout.write(line)
