from pyspark import SparkContext
import sys

idig_path = "idigbio_small"
if len(sys.argv) > 1:
    idig_path = sys.argv[1]

sc = SparkContext(appName="dq_test")

ll = lambda s: 1
add_up = lambda a, b: a + b
split_ott = lambda s: [c.strip() for c in s.split("|")]
split_gbif = lambda s: [c.strip() for c in s.split("\t")]
split_idigbio = lambda s: [c.strip() for c in s.split(",")]
filter_species_ott = lambda a: a[3] == "species"
filter_species_gbif = lambda a: a[5] == "species"
get_ott_name_id = lambda a: (a[2], a[0])
get_gbif_name_id = lambda a: (a[4], a[0])
get_idigbio_name_id = lambda a: (a[1][0].upper() + a[1][1:] + " " + a[2],a[0])

gbif = sc.textFile("backbone/taxon.txt")
gb_names = gbif.map(split_gbif).filter(filter_species_gbif).map(get_gbif_name_id)
gb_names.cache()
gb_total = gb_names.map(ll).reduce(add_up)

ott = sc.textFile("ott2.8/taxonomy.tsv")
ott_names = ott.map(split_ott).filter(filter_species_ott).map(get_ott_name_id)
ott_names.cache()
ot_total = ott_names.map(ll).reduce(add_up)

print "GBIF Species Count", gb_total
print "OTT Species Count", ot_total

idigbio = sc.textFile("{0}/occurrence.csv".format(idig_path))
idigbio_names = idigbio.map(split_idigbio).map(get_idigbio_name_id)
idigbio_names.cache()
gb_ids = idigbio_names.join(gb_names)
gb_ids.saveAsTextFile("gbif_ids")
ott_ids = idigbio_names.join(ott_names)
ott_ids.saveAsTextFile("ott_ids")
