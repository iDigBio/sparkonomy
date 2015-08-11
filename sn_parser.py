from pyspark import SparkContext, SQLContext
import sys
import re
import nltk.tokenize
import pyspark_csv as pycsv

sc = SparkContext(appName="sn_parse")
sqlCtx = SQLContext(sc)
sc.addPyFile('pyspark_csv.py')

split_gbif = lambda s: [c.strip() for c in s.split("\t")]
split_idigbio = lambda s: [c.strip() for c in s.split(",")]
get_idigbio_words = lambda a: [w.lower() for w in nltk.tokenize.wordpunct_tokenize(a[1])]  
count = lambda v1, v2: v1 + v2

numeric = re.compile("\d+")
alpha = re.compile("\w+")

def get_gbif_word_type(a):
    tokens = []
    try:
        cn_words = nltk.tokenize.wordpunct_tokenize(a[4])
        sn_words = nltk.tokenize.wordpunct_tokenize(a[3])
        tokens.append((cn_words[-1].lower(), str(a[5].upper())))
        for aw in sn_words:
            if aw in cn_words or not alpha.match(aw):
                continue
            tokens.append((aw.lower(), "AUTHOR"))
    except:
        print a
    return tokens

def get_idigbio_words_extended(a):
    tokens = []
    if a[1] is not None:
        for i, w in enumerate(nltk.tokenize.wordpunct_tokenize(a[1])):
            tokens.append((w.lower(), (a[0],i)))
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
    passes = 2
    while passes > 0:
        if len(pa) <= 1:
            break
        i = 0
        t_out = []
        #pair wise matches
        while i < len(pa):
            if i+1 < len(pa):
                if (pa[i] == "GENUS" or "GENUS" in pa[i]) and (pa[i+1] == "SPECIES" or "SPECIES" in pa[i+1]):
                    t_out.append("GENUS SPECIES")
                    i += 2
                # Mark abbrv + period consumers
                elif (pa[i] == "SP_M" or "SP_M" in pa[i]) and pa[i+1] == "PERIOD":
                    t_out.append("SP_MARK")
                    i += 2
                elif (pa[i] == "GEN_M" or "GEN_M" in pa[i]) and pa[i+1] == "PERIOD":
                    t_out.append("GEN_MARK")
                    i += 2
                elif (pa[i] == "VAR_M" or "VAR_M" in pa[i]) and pa[i+1] == "PERIOD":
                    t_out.append("VAR_MARK")
                    i += 2
                elif (pa[i] == "FORM_M" or "FORM_M" in pa[i]) and pa[i+1] == "PERIOD":
                    t_out.append("FORM_MARK")
                    i += 2
                # Mark + item consumers
                elif pa[i] == "SP_MARK" and (pa[i+1] == "SPECIES" or "SPECIES" in pa[i+1]):
                    t_out.append("SP_MARK SPECIES")
                    i += 2
                elif pa[i] == "GEN_MARK" and (pa[i+1] == "GENUS" or "GENUS" in pa[i+1]):
                    t_out.append("GEN_MARK GENUS")
                    i += 2
                elif pa[i] == "VAR_MARK" and (pa[i+1] == "VARIETY" or "VARIETY" in pa[i+1]):
                    t_out.append("VAR_MARK VARIETY")
                    i += 2
                elif pa[i] == "FORM_MARK" and (pa[i+1] == "FORM" or "FORM" in pa[i+1]):
                    t_out.append("FORM_MARK FORM")
                    i += 2                    
                else:
                    t_out.append(pa[i])
                    i += 1                    
            else:
                t_out.append(pa[i])
                i += 1
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

def parse_named_tokens(s):
    a = s.split(" ")
    return (a[0], [a[1]])

prep_count = lambda t: (t[0], 1)

named_tokens = sc.textFile("named_tokens.txt").map(parse_named_tokens)
gbif = sc.textFile("backbone/taxon.txt")
gb_names = gbif.map(split_gbif).flatMap(get_gbif_word_type).aggregateByKey([],lambda a, s: list(set(a + [s])),lambda a1,a2: list(set(a1 + a2))).union(named_tokens)

idigbio = sc.textFile("uniquenames.csv")
idigbio_names = pycsv.csvToDataFrame(sqlCtx,idigbio)
idigbio_words = idigbio_names.flatMap(get_idigbio_words_extended)
idigbio_tokens = idigbio_words.leftOuterJoin(gb_names)
idigbio_tokens.cache()
idigbio_tokens.saveAsTextFile("sn_parsed_intermediate")
idigbio_labeled = idigbio_tokens.map(un_tuple).aggregateByKey([],lambda a, s: a + [s], lambda a1, a2: a1 + a2).map(parts_in_order)
idigbio_labeled.cache()

idigbio_labeled.saveAsTextFile("sn_parsed_format_by_key")
idigbio_labeled.map(prep_count).reduceByKey(count).saveAsTextFile("sn_parsed")