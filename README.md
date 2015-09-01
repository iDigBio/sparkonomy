# sparkonomy
Taxonomic Parsing in Spark

## To run:
* Get spark set up to the point where you can run python scripts with spark-submit, and have spark-submit in your path
* Install dependencies from requirements.txt on your workers. (sudo pip install -r requirements.txt for running locally)
* Get the data:
 * wget -O uniquenames.csv http://s.idigbio.org/idigbio-downloads/b7ed1a95-ae0e-4cd7-b1b2-de854668b78e.uniquenames.csv (or any other id, scientificName csv file)
 * wget http://rs.gbif.org/datasets/backbone/backbone.zip
 * unzip backbone.zip -d backbone
* ./run.sh
* Output:
 * sn_parsed_massaged.txt: detected formats in order of rank
 * sn_parsed_intermediate.txt: intermediate token detection (mainly for debug)
 * sn_parsed_format_by_key.txt: list of format, taxon_id pairs (using the id as the first column of the uniquenames file)

## Performance

Performance on a 4 core 16GB desktop machine w/ 10GB of memory given to spark on idigbio's 1.7 million name dataset:

    real    4m14.021s
    user    1m3.244s
    sys 0m6.006s

About half of that is fixed setup time loading and parsing the GBIF taxonomy.
