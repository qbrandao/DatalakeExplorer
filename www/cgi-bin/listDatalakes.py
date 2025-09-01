import pathlib

def listDatalakes():
    base_path = pathlib.Path(f"/home/qbrandao/Datalakes/")
    ListDatalake = [f.name for f in base_path.iterdir() if f.is_dir()] if base_path.exists() else []

    return {
        "ListDatalake": ListDatalake
    }
