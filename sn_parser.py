from pyspark import SparkContext, SQLContext
import sys
import re
import nltk.tokenize
import pyspark_csv as pycsv
import os

sc = SparkContext(appName="sn_parse")
sqlCtx = SQLContext(sc)
sc.addPyFile('pyspark_csv.py')

# This should probably be pyspark csv as well
split_gbif = lambda s: [c.strip() for c in s.split("\t")]
count = lambda v1, v2: v1 + v2
prep_count = lambda t: (t[0], t[1][1])

numeric = re.compile("\d+")
alpha = re.compile("\w+")

def parse_named_tokens(s):
    a = s.split(" ")
    return (a[0], str(a[1]))

named_tokens = sc.textFile("named_tokens.txt").map(parse_named_tokens)
named_tokens.cache()

named_token_words = sc.broadcast(named_tokens.map(lambda t: t[0]).collect())

def get_gbif_word_type(a):
    tokens = []
    try:
        if a[4] != "":
            cn_words = [w.lower() for w in nltk.tokenize.wordpunct_tokenize(a[4])]
        else:
            cn_words = []

        if a[3] != "":
            sn_words = [w.lower() for w in nltk.tokenize.wordpunct_tokenize(a[3])]
        else:
            sn_words = []

        if len(cn_words) > 0 and cn_words[-1] not in named_token_words.value:
            tokens.append((cn_words[-1], str(a[5].upper())))

        for aw in sn_words:
            if aw not in cn_words and alpha.match(aw) and aw not in named_token_words.value:
                tokens.append((aw, "AUTHOR"))
    except:
        print a
    return tokens

def get_idigbio_words_extended(a):
    tokens = []
    if a[1] is not None:
        for i, w in enumerate(nltk.tokenize.wordpunct_tokenize(a[1])):
            tokens.append((w.lower(), ((a[0],int(a[2])),i)))
    return tokens

def un_tuple(t):
    t_ar = t[1][1]
    sn_k = t[1][0][0]
    sn_i = t[1][0][1]
    if t_ar is None:
        if numeric.search(t[0]) is not None:
            return (sn_k, (sn_i, "NUMERIC"))
        else:
            return (sn_k, (sn_i, "NONE"))
    elif len(t_ar) == 1:
        return (sn_k, (sn_i, t_ar[0])) 
    else:
        return (sn_k, (sn_i, t_ar))

def parts_in_order(t):
    k = t[0]
    pa = t[1]
    pa = [v[1] for v in sorted(pa,key=lambda v: v[0])]
    passes = 1
    while passes > 0:
        if len(pa) <= 1:
            break
        i = 0
        t_out = []
        #pair wise matches
        while i < len(pa):
            #print i, len(pa), pa[i:]
            consumed = False
            if not consumed and i+2 < len(pa):
                if (pa[i+1] == "PERIOD" or "PERIOD" in pa[i+1]):
                    mark = None
                    if isinstance(pa[i],str):
                        if pa[i].endswith("_M"):
                            mark = pa[i]
                    else:
                        for c in pa[i]:
                            if c.endswith("_M"):
                                mark = c
                                break

                    if mark is not None:
                        mt = mark.split("_")[0]
                        if pa[i+2] == mt or mt in pa[i+2]:
                            t_out.extend(pa[i:i+2] + [mt])
                            i += 3
                            consumed = True
                elif (pa[i] == "GENUS" or "GENUS" in pa[i]) and (pa[i+1] == "SPECIES" or "SPECIES" in pa[i+1]) and (pa[i+2] == "SUBSPECIES" or "SUBSPECIES" in pa[i+2]):
                    t_out.extend(["GENUS","SPECIES","SUBSPECIES"])
                    i += 3
                    consumed = True

            if not consumed and i+1 < len(pa):
                if (pa[i] == "GENUS" or "GENUS" in pa[i]) and (pa[i+1] == "SPECIES" or "SPECIES" in pa[i+1]):
                    t_out.extend(["GENUS","SPECIES"])
                    i += 2
                    consumed = True

            if not consumed:
                t_out.append(pa[i])
                i += 1
            #print consumed, t_out
        passes -= 1
        pa = t_out        

    # Cleanup to strings
    t_out = []
    for t in pa:
        if isinstance(t,list):
            t_out.append("AMBIGUOUS")
        else:
            t_out.append(t)
    pa = t_out

    return (str(" ".join(pa)), k)

def reverse_labels(t):
    return (t[1][0], t[0])

def names_as_tuples(r):
    return (r[0], (r[1], r[2]))

def merge_labels_name(t):
    a = []
    labels = t[1][1].split(" ")
    if len(labels) > 0:
        tokens = nltk.wordpunct_tokenize(t[1][0][0])
        for i, tok in enumerate(tokens):
            a.append((tok, labels[i]))

    return (t[0], a)

gbif = sc.textFile("backbone/taxon.txt")
gb_names = gbif.map(split_gbif).flatMap(get_gbif_word_type).union(named_tokens).aggregateByKey([],lambda a, s: list(set(a + [s])),lambda a1,a2: list(set(a1 + a2)))

idigbio = sc.textFile("uniquenames.csv")
idigbio_names = pycsv.csvToDataFrame(sqlCtx,idigbio)
idigbio_names.cache()
idigbio_words = idigbio_names.flatMap(get_idigbio_words_extended)
idigbio_tokens = idigbio_words.leftOuterJoin(gb_names)
# idigbio_tokens.cache()
# idigbio_tokens.saveAsTextFile("sn_parsed_intermediate")
idigbio_labeled = idigbio_tokens.map(un_tuple).aggregateByKey([],lambda a, s: a + [s], lambda a1, a2: a1 + a2).map(parts_in_order)
idigbio_labeled.cache()

idigbio_labeled.saveAsTextFile("sn_parsed_format_by_key")
idigbio_labeled.map(prep_count).reduceByKey(count).saveAsTextFile("sn_parsed")

idigbio_labeled_rev = idigbio_labeled.map(reverse_labels)
idigbio_parsed = idigbio_names.map(names_as_tuples).join(idigbio_labeled_rev).map(merge_labels_name)
idigbio_parsed.saveAsTextFile("uniquenames_parsed")