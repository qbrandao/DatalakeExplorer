import pathlib
import sake # type: ignore

def parquet2json_more(datalake, chrom, start, end):
    sakepath = pathlib.Path(f"/home/qbrandao/Downloads/" + datalake)
    sakedb = sake.Sake(sakepath, ".")
    lchrom = chrom
    lstart = list(map(int, start))
    lend = list(map(int, end))
    print(f"Calling get_intervals with chromosomes: {lchrom}, starts: {lstart}, ends: {lend}")
    df = sakedb.get_intervals(lchrom, lstart, lend)
    df = sakedb.add_annotations(df, "snpeff", "5.2f", select_columns=["effect", "impact","gene","geneid","feature","feature_id","bio_type","rank","hgvs_c","hgvs_p","cdna_pos","cdna_len","cds_pos","cds_len","aa_pos"], read_threads=2)
    df = sakedb.add_genotypes(df)
    return df.to_dicts()
