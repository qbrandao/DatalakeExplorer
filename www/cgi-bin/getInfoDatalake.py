import pathlib
import polars # type: ignore

def get_info(datalake):
    base_path = pathlib.Path(f"/home/qbrandao/Datalakes/"+datalake)

    patPath = base_path / "genotypes/samples/"
    ListPat = [f.name for f in patPath.iterdir() if f.is_file()] if patPath.exists() else []
    NbPat = len(ListPat)

    def get_latest_version(path: pathlib.Path) -> str:
        if not path.exists():
            return "Not Available"
        subdirs = [f for f in path.iterdir() if f.is_dir()]
        return pathlib.Path(max(subdirs)).name if subdirs else "Not Available"

    SnpEff_v = get_latest_version(base_path / "annotations/snpeff/")
    hgmd_v = get_latest_version(base_path / "annotations/hgmd/")
    gnomAD_v = get_latest_version(base_path / "annotations/gnomad/")
    Clinvar_v = get_latest_version(base_path / "annotations/clinvar/")

    data = {
        "SnpEff_v": SnpEff_v,
        "gnomAD_v": gnomAD_v,
        "Clinvar_v": Clinvar_v,
        "hgmd_v": hgmd_v,
        "NbPat": NbPat,
        "ListPat": ListPat
    }

    df = polars.DataFrame([data])
    return df.to_dicts()
