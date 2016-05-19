# Flink-based DBpedia Data Slicer

**DISCLAIMER:** This this software is not currently not in a state that would warrant an official release. It's
  primarily and ad-hoc tool for a very specific task, although it implements operation primitives that could be
  combined for similar tasks in a useful manner. I also suspect that there is still a lot of room for improvement
  in overall performance.
  
  
## Quick Manual
1. Clone the repo (and install the SBT Launcher on your system, if not already present)
2. Download and unpack the Flink distribution (with the matching version, currently 1.0.3)
3. Prepare DBpedia dumps to be sliced (as described in a seperate section)
4. Write an `.properties`-config file for your slice job (as described in a seperate section)
5. Use SBT to prepare a `.jar` with the Flink Slicing program: `sbt assembly` 
   (resulting jar: `target/scala-2.11/dbp-flink-slicing.jar`)
6. Start the Flink Environment (e.g. a local environment with `flink-1.0.3/bin/start-local.sh`)
7. Submit your configured Flink program: 
   `flink-1.0.3/bin/flink run /path/to/dbp-flink-slicing.jar /path/to/config.properties`
   
## Preparing the DBpedia Dumps
1. Pick a DBpedia language version and the dump files relevant for you (the `instance-types` dump file is required)
2. Download the corresponding `ttl` dump files into a single directory (the `dbpedia.dump.dir`)
3. Extract the bzipped dump files (`pbzip2` recommended for faster extraction through multi-core usage)
4. Use `cat` to create a single combined file of all triples you want to slice from (recommended filename: `combined.ttl`)


## Preparing the Slice Configuration
Configuration for the slicing process (mainly input and output location) are read from a `.properties` file.
Possible keys are:

`dbpedia.dump.dir` (mandatory): Directory containing the prepared DBpedia dump

`slice.destination.dir` (mandatory): Output directory for the slice results as uncompressed NTriples files

`combined.statements.filename` (optional): see *Preparing the DBpedia Dumps section* (default: `combined.ttl`) 

`instance.types.filename` (optional): the filname of the mapping-based instance type assertions (default: `instance-types_en.ttl`)

`distribution.description.infix` (optional): an optional filename infix for the slice result files (e.g. to avoid name clashed for 
  slice results for different language versions (default: no infix)

`external.ontology.path` (optional): full path to an NTriples file containing the corresponding DBpedia ontology for the dump
  
* the ontology file can also be compressed with gzip or bzip2; 
* by default, a the DBpedia 2015-10 ontology is bundled in the project `jar` and will be used

For example a configuration file might look like:

```
dbpedia.dump.dir=/data/dbpedia-dumps/de-2015-10-en-uris
slice.destination.dir=/data/dbp-slicing/2015-10/de-en-uris-test
combined.statements.filename=combined.ttl
instance.types.filename=instance_types_en_uris_de.ttl
distribution.description.infix=_en_uris_de
external.ontology.path=/data/dbpedia-dumps/dbpedia_ont_2015-10.nt
```
