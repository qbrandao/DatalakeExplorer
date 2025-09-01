import pathlib

def get_exome_bed():
    base_path = pathlib.Path(f"/mnt/references/Whole_Genes_gencode_v48.no_annot.bed")
    with open(base_path, "r") as f:
        exome_pos_list = [line.strip() for line in f if line.strip()]
    return {"exome_pos": exome_pos_list}
