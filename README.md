# RML-Planner

## How to Execute
```
python3 run_planner.py /path/to/config
```

Example of Configuration File
```
[default]
main_directory: .

[datasets]
number_of_datasets: 1
output_folder: ${default:main_directory}/output
engine: SDM-RDFizer

[dataset1]
name: Test
mapping: ${default:main_directory}/mapping.ttl
```

## Configuration File Parameters
- main_directory: path to folder containing the mappings to be semantify.
- number_of_datasets: number of mappings to be semantify.
- output_folder: Folder where the output Knowledge Graph will be generated.
- engine: indicates which engine the RML-Planner will execute (available engines: SDM-RDFizer, RMLMapper, RocketRML, and Morph-KGC).
- name: Name of the nt file that will be generated.
- mapping: Location and name of the mapping file to be semantify. Please note, that location of the data sources that the mapping requires must be defined in the mapping itself.

## Important Information
This repository only contains the RML-Planner. The knowledge graph creation engines that can be used along side this planner must be downloaded separately.