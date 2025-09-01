#!/bin/bash
set -e

PATH_VCF="/mnt/shared_folder/VCFs/"
CONTIGS="/mnt/references/grch38.92.csv"
DATALAKENAME="datalake_LR"

mkdir -p $DATALAKENAME/vcf/
cp ${PATH_VCF}/*vcf* $DATALAKENAME/vcf/
cd $DATALAKENAME
mkdir -p variants genotypes/samples/
#vcf2parquet de tous les vcf dans le repertoire "vcf" 
for vcf_path in $(ls vcf/*.vcf*)
do
    string=$(basename "$vcf_path")
    sample_name="${string%%_*}"
    variantplaner -t 4 vcf2parquet -i ${vcf_path} \
    variants -o variants/${sample_name}.parquet \
    genotypes -o genotypes/samples/${sample_name}.parquet
done
variantplaner -t 8 struct -i variants/*.parquet -- variants -o variants
mkdir -p genotypes/partitions/
variantplaner -t 8 struct -i genotypes/samples/*.parquet -- genotypes -p genotypes/partitions

###snpeff
for parquet in $(ls variants/chr*.parquet); do   sample_name=$(basename ${parquet} .parquet);   variantplaner -t 8 parquet2vcf -v ${parquet} -o vcf/${sample_name}.vcf; done
cd vcf
grep ^# chr1.vcf >variants.vcf
for file in $(ls chr*.vcf); do grep ^chr $file >>variants.vcf; done 
cd ..
snpeff_version=$(
  java -jar /home/qbrandao/Downloads/snpEff/snpEff.jar 2>&1 |
  grep "SnpEff version" |
  awk '{print $4}'
)
mkdir -p annotations/snpeff/$snpeff_version
java -Xmx8g -jar /home/qbrandao/Downloads/snpEff/snpEff.jar -v GRCh38.86 vcf/variants.vcf > vcf/variants.ann.vcf
#découper variants.ann.vcf / chr
for chr in $(grep -v "^#" vcf/variants.ann.vcf | cut -f1 | sort -u); do
    (
        grep "^#" vcf/variants.ann.vcf
        grep -w "^${chr}" vcf/variants.ann.vcf
    ) > "annotations/snpeff/$snpeff_version/${chr}.vcf"
done
for chr in $(ls annotations/snpeff/$snpeff_version/*vcf); do file=$(basename $chr .vcf); python /home/qbrandao/Downloads/snpeffInParquet.py ${chr} annotations/snpeff/$snpeff_version/${file}.parquet; done

#préparer variants.vcf pour annoter avec bcftools (pour garder les id variantplaner)
grep ^chr vcf/variants.vcf >vcf/variants_headerless.vcf
cat /mnt/references/header_variantplaner vcf/variants_headerless.vcf >vcf/variants_header.vcf
bcftools sort vcf/variants_header.vcf -Oz -o vcf/variants.sorted.vcf.gz
tabix -p vcf vcf/variants.sorted.vcf.gz

bcftools norm -m -both -Oz -o vcf/variants.sorted.norm.vcf.gz vcf/variants.sorted.vcf.gz
tabix -p vcf vcf/variants.sorted.norm.vcf.gz

###Clinvar
mkdir -p annotations/clinvar/
curl https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz | gunzip - > annotations/clinvar/clinvar.vcf &&\
# ajouter chr devant les numéros des chromosomes
awk 'BEGIN{OFS="\t"} \
     /^#/ {print $0; next} \
     { $1 = "chr"$1; print }' annotations/clinvar/clinvar.vcf > annotations/clinvar/clinvar_fixed.vcf
clinvar_date=$(grep -m1 "^##fileDate=" annotations/clinvar/clinvar_fixed.vcf | cut -d'=' -f2)
path_annot_clinvar="annotations/clinvar/${clinvar_date}/"
mkdir -p $path_annot_clinvar
bgzip annotations/clinvar/clinvar_fixed.vcf
tabix -p vcf annotations/clinvar/clinvar_fixed.vcf.gz
bcftools annotate \
  -a annotations/clinvar/clinvar_fixed.vcf.gz \
  -c INFO \
  vcf/variants.sorted.norm.vcf.gz \
  -O z -o vcf/variants.ann.clinvar.vcf.gz
#ici couper le vcf clinvar par chr
for chr in $(zgrep -v "^#" annotations/clinvar/clinvar_fixed.vcf.gz | cut -f1 | sort -u); do
    (
        zgrep "^#" annotations/clinvar/clinvar_fixed.vcf.gz
        zgrep -w "^${chr}" vcf/variants.ann.clinvar.vcf.gz               
    ) > "$path_annot_clinvar/${chr}.vcf"
done
rm $path_annot_clinvar/chrN*
for f in $(ls $path_annot_clinvar/chr*.vcf);do name=$(basename $f .vcf); variantplaner vcf2parquet -c /mnt/references/grch38.92.csv -i $f annotations -o $path_annot_clinvar/$name.parquet -r clinvar_id -i CLNSIG -i CLNSIGCONF; done
for f in $(ls $path_annot_clinvar/chr*.parquet); do
    fname=$(basename $f);
    tmp_file="$path_annot_clinvar/tmp_$fname"
    duckdb -c "
        COPY (
            SELECT 
                CAST(clinvar_id AS UINT64) AS id,
                id AS old_id,
                CLNSIG,
                CLNSIGCONF
            FROM '$f'
        )
        TO '$tmp_file' (FORMAT 'parquet');
    "
    mv "$path_annot_clinvar/tmp_$fname" "$f";
done
###HGMD
hgmd_file="/mnt/references/HGMD_Pro_2023.3_hg38_CLASS.vcf.gz"
hgmd_date=$(zgrep -m1 "^##source=" ${hgmd_file} | cut -d'=' -f2)
path_annot_hgmd="annotations/hgmd/${hgmd_date}/"
mkdir -p $path_annot_hgmd
bcftools annotate \
  -a ${hgmd_file} \
  -c INFO \
  vcf/variants.sorted.norm.vcf.gz \
  -O z -o vcf/variants.ann.hgmd.vcf.gz
#ici couper le vcf hgmd par chr
for chr in $(zgrep -v "^#" ${hgmd_file} | cut -f1 | sort -u); do
    (
        zgrep "^#" vcf/variants.ann.hgmd.vcf.gz
        zgrep -w "^${chr}" vcf/variants.ann.hgmd.vcf.gz
    ) > "annotations/hgmd/${hgmd_date}/${chr}.vcf"
done
for f in $(ls $path_annot_hgmd/chr*.vcf);do name=$(basename $f .vcf); variantplaner vcf2parquet -c /mnt/references/grch38.92.csv -i $f annotations -o $path_annot_hgmd/$name.parquet -r hgmd_id -i CLASS; done
for f in $(ls $path_annot_hgmd/chr*.parquet); do
    fname=$(basename $f);
    tmp_file="$path_annot_hgmd/tmp_$fname"
    duckdb -c "
        COPY (
            SELECT 
                CAST(hgmd_id AS UINT64) AS id,
                id AS old_id,
                CLASS
            FROM '$f'
        )
        TO '$tmp_file' (FORMAT 'parquet');
    ";
    mv "$path_annot_hgmd/tmp_$fname" "$f";
done
###gnomAD
gnomad_file="/mnt/references/merge_exom_genom_all_chr_header.vcf.gz"
path_annot_gnomad="annotations/gnomad/merge_exom2.1.1_genom3.1.2/"
mkdir -p $path_annot_gnomad
bcftools annotate \
  -a ${gnomad_file} \
  -c INFO \
  vcf/variants.sorted.norm.vcf.gz \
  -O z -o vcf/variants.ann.gnomad.vcf.gz
for chr in $(zgrep -v "^#" ${gnomad_file} | cut -f1 | sort -u); do
    (
        zgrep "^#" ${gnomad_file}
        zgrep -w "^${chr}" vcf/variants.ann.gnomad.vcf.gz
    ) > "$path_annot_gnomad/${chr}.vcf"
done
for f in $(ls $path_annot_gnomad/chr*.vcf); do name=$(basename $f .vcf); variantplaner vcf2parquet -c /mnt/references/grch38.92.csv -i $f annotations -o $path_annot_gnomad/$name.parquet -r gnomad_id; done

for f in "$path_annot_gnomad"/chr*.parquet; do
    fname=$(basename "$f")
    tmp_file="$path_annot_gnomad/tmp_$fname"
    duckdb -c "
        COPY (
            SELECT 
                CAST(gnomad_id AS UINT64) AS id,
                id AS old_id,
                AC,
                AN,
                AF,
                het,
                homhem
            FROM '$f'
        )
        TO '$tmp_file' (FORMAT 'parquet');
    "
    if [ -f "$tmp_file" ]; then
        mv "$tmp_file" "$f"
    else
        echo "Erreur : fichier temporaire non généré pour $f"
    fi
done
