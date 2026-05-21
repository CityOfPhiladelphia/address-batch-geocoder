import streamlit as st
import re
from pathlib import Path


def parse_how_to_md(file_text: list):
    parsed = []
    current_group = []

    for line in file_text:
        if re.search("(?<=\[)image.*?(?=\])", line):
            img_path = re.search("(?<=\().*?(?=\))", line).group()
            parsed.append(st.markdown("".join(current_group)))
            current_group = []
            parsed.append(st.image(img_path))
        else:
            current_group.append(line)

    if current_group:
        parsed.append(st.markdown("".join(current_group)))

    return parsed


def render_how_to_page(filepath: Path):
    file_text = open(filepath).readlines()

    parsed = parse_how_to_md(file_text)

    return parsed
