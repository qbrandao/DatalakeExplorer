import pathlib
import sake # type: ignore

def parquet2json(datalake, chrom, start, end):
    sakepath = pathlib.Path(f"/home/qbrandao/Datalakes/"+datalake)
    sakedb = sake.Sake(sakepath,".")

    df = sakedb.get_interval(chrom, start, end)
    
    df = sakedb.add_annotations(df,"snpeff","5.2f",select_columns=["effect", "impact","gene","geneid","feature","feature_id","bio_type","rank","hgvs_c","hgvs_p","cdna_pos","cdna_len","cds_pos","cds_len","aa_pos"],read_threads=8)
    df = sakedb.add_annotations(df,"clinvar","2025-07-29",read_threads=8)
    df = sakedb.add_annotations(df,"hgmd","HGMD_PRO_2023.3",read_threads=8)
    df = sakedb.add_annotations(df,"gnomad","merge_exom2.1.1_genom3.1.2",read_threads=8)
    df = sakedb.add_genotypes(df)

    return df.to_dicts()
