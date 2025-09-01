import pathlib


def get_mane():
    base_path = pathlib.Path("/mnt/references/MANE_ENST.txt")
    with open(base_path, "r") as f:
        enst_list = [line.strip() for line in f if line.strip()]
    return {"transcripts": enst_list}
