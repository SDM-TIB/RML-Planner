import re
import csv
import sys
import os
try:
    from tree import Node as tree
except:
    from .tree import Node as tree
import subprocess

global prefixes
prefixes = {}
global general_predicates
general_predicates = {"http://www.w3.org/2000/01/rdf-schema#subClassOf":"",
                        "http://www.w3.org/2002/07/owl#sameAs":"",
                        "http://www.w3.org/2000/01/rdf-schema#seeAlso":"",
                        "http://www.w3.org/2000/01/rdf-schema#subPropertyOf":"",
                        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type":""}
global outputs
outputs = {}
global index
index = 0

def config_writer(engine, output, mapping):
    if "SDM-RDFizer" == engine:
        config_file = "[default]\nmain_directory: "
        config_file += output + "\n\n"
        config_file += "[datasets]\nnumber_of_datasets: 1\noutput_folder: ${default:main_directory}/\nremove_duplicate: yes\nall_in_one_file: no\nenrichment: yes\nname: "
        config_file += mapping.split(".")[0] + "\n"
        config_file += "ordered: yes\nlarge_file: false\n\n"
        config_file += "[dataset1]\nname: "
        config_file += mapping.split(".")[0] + "\n"
        config_file += "mapping: " + output + "/" + mapping + "\n"
        config_name = output + "/" + "configfile_" + mapping.split(".")[0] + ".ini"
    elif "RocketRML" == engine:
        config_file = "const parser = require('rocketrml');\nconst doMapping = async () => {\n  const options = {\n    toRDF: true,\n"
        config_file += "    verbose: true,\n    xmlPerformanceMode: false,\n    replace: false,\n  };\n"
        config_file += "  const result = await parser.parseFile(\'"
        config_file += output + "/" + mapping + "\', \'" + output + "/" + mapping.split(".")[0] + ".nt\', options).catch((err) => { console.log(err); });\n"
        config_file += "  console.log(result);\n};\n\ndoMapping();"
        config_name = output + "/" + "configfile_" + mapping.split(".")[0] + ".js"
    elif "Morph-KGC" == engine:
        config_file = "[CONFIGURATION]\n\n# OUTPUT\noutput_dir="
        config_file += output + "/" + mapping.split(".")[0] + "\n"
        config_file += "output_file="
        config_file += mapping.split(".")[0] + "\n"
        config_file += "output_format=N-TRIPLES\n\n[DataSource1]\nsource_type=CSV\nmappings="
        config_file += output + "/" + mapping
        config_name = output + "/" + "configfile_" + mapping.split(".")[0] + ".ini"
    mapping_file = open(config_name,"w")
    mapping_file.write(config_file)
    mapping_file.close()
    return config_name

def partitions_clasification(partitions):
    clasified_partitions = {"sort":[],"cat":[]}
    for node in partitions:
        for neighbor in partitions[node]:
            if partitions[node][neighbor] != 0:
                if node not in clasified_partitions["sort"]:
                   clasified_partitions["sort"].append(node)
                if neighbor not in clasified_partitions["sort"]:
                   clasified_partitions["sort"].append(neighbor)
    for node in partitions:
        if node not in clasified_partitions["sort"]:
            clasified_partitions["cat"].append(node)
    return clasified_partitions

def execute_partitions(engine, clasified_partitions, output, output_name):
    if "Morph-KGC" == engine:
        wait = "wait "
        command = ""
        i = 1
        for partitions in clasified_partitions["cat"]:
            command += "timeout 5h python3 -m morph_kgc " + config_writer(engine, output, partitions) + " & "
            wait += "%" + str(i) + " "
            i += 1
        for partitions in clasified_partitions["sort"]:
            command += "timeout 5h python3 -m morph_kgc " + config_writer(engine, output, partitions) + " & "
            wait += "%" + str(i) + " "
            i += 1
        command = command + wait
        os.system(command)
        cat = ""
        sort = ""
        if len(clasified_partitions["cat"]) != 0:
            cat = "cat "
            for partitions in clasified_partitions["cat"]:
                if os.path.isfile(output + "/" + partitions.split(".")[0] + "/" + partitions.split(".")[0] + ".nt"):
                    cat += output + "/" + partitions.split(".")[0] + "/"  + partitions.split(".")[0] + ".nt "
            cat += " > " + output + "/"  + output_name + ".nt"
        if len(clasified_partitions["sort"]) != 0:
            if len(clasified_partitions["cat"]) != 0:
                sort = " && sort -u "
            else:
                sort = " sort -u "
            for partitions in clasified_partitions["sort"]:
                if os.path.isfile(output + "/" + partitions.split(".")[0] + "/" + partitions.split(".")[0] + ".nt"):
                    sort += output + "/" + partitions.split(".")[0] + "/" + partitions.split(".")[0] + ".nt "
            if len(clasified_partitions["cat"]) != 0:
                sort += " >> " + output + "/" + output_name + ".nt"
            else:
                sort += " > " + output + "/" + output_name + ".nt"
        os.system(cat + sort)
    else:
        wait = "wait "
        command = ""
        i = 1
        if "SDM-RDFizer" == engine:
            for partitions in clasified_partitions["cat"]:
                command += "timeout 5h python3 -m rdfizer -c " + config_writer(engine, output, partitions) + " & "
                wait += "%" + str(i) + " "
                i += 1
            for partitions in clasified_partitions["sort"]:
                command += "timeout 5h python3 -m rdfizer -c " + config_writer(engine, output, partitions) + " & "
                wait += "%" + str(i) + " "
                i += 1
        elif "RMLMapper" == engine:
            for partitions in clasified_partitions["cat"]:
                command += "timeout 5h java -jar rmlmapper.jar -m "
                command += output + "/" + partitions + " -o "
                command += output + "/" + partitions.split(".")[0] + ".nt & "
                wait += "%" + str(i) + " "
                i += 1
            for partitions in clasified_partitions["sort"]:
                command += "timeout 5h java -jar rmlmapper.jar -m "
                command += output + "/" + partitions + " -o "
                command += output + "/" + partitions.split(".")[0] + ".nt & "
                wait += "%" + str(i) + " "
                i += 1
        elif "RocketRML" == engine:
            for partitions in clasified_partitions["cat"]:
                command += "timeout 5h node --max-old-space-size=32000 " + config_writer(engine, output, partitions) + " & "
                wait += "%" + str(i) + " "
                i += 1
            for partitions in clasified_partitions["sort"]:
                command += "timeout 5h node --max-old-space-size=32000 " + config_writer(engine, output, partitions) + " & "
                wait += "%" + str(i) + " "
                i += 1
        else:
            print("The " + config["datasets"]["engine"] + " is not a compatible tool.")
            print("Aborting...")
            sys.exit(1)
        command = command + wait
        os.system(command)
        cat = ""
        sort = ""
        if len(clasified_partitions["cat"]) != 0:
            cat = "cat "
            for partitions in clasified_partitions["cat"]:
                if os.path.isfile(output + "/" + partitions.split(".")[0] + ".nt"):
                    cat += output + "/" + partitions.split(".")[0] + ".nt "
            cat += " > " + output + "/"  + output_name + ".nt"
        if len(clasified_partitions["sort"]) != 0:
            if len(clasified_partitions["cat"]) != 0:
                sort = " && sort -u "
            else:
                sort = " sort -u "
            for partitions in clasified_partitions["sort"]:
                if os.path.isfile(output + "/" + partitions.split(".")[0] + ".nt"):
                    sort += output + "/" + partitions.split(".")[0] + ".nt "
            if len(clasified_partitions["cat"]) != 0:
                sort += " >> " + output + "/" + output_name + ".nt"
            else:
                sort += " > " + output + "/"+ output_name + ".nt"
        os.system(cat + sort)

def neighborhood_table(partitions):
    neighborhood = {}
    for node in partitions:
        neighborhood[node] = {}
        for neighbor in partitions:
            if node != neighbor:
                neighborhood[node][neighbor] = len(partitions[node].keys() & partitions[neighbor].keys())
    return neighborhood



def string_separetion(string):
    if ("{" in string) and ("[" in string):
        prefix = string.split("{")[0]
        condition = string.split("{")[1].split("}")[0]
        postfix = string.split("{")[1].split("}")[1]
        field = prefix + "*" + postfix
    elif "[" in string:
        return string, string
    else:
        return string, ""
    return string, condition

def grouping_mappings(triples_map_list):
    grouping = {}
    for triples_map in triples_map_list:
        if triples_map.data_source not in grouping:
            grouping[triples_map.data_source] = [triples_map.triples_map_id]
        else:
            grouping[triples_map.data_source].append(triples_map.triples_map_id)
    return grouping

def update_mapping(original, output_folder, number, mapping_group, triples_map_list):
    mapping = ""
    parent_triples_maps = {}
    predicates = {}

    for tm in mapping_group:
        for triples_map in triples_map_list:
            if tm == triples_map.triples_map_id:
                mapping += "<" + triples_map.triples_map_id + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
                if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None":
                    mapping += "                rml:referenceFormulation ql:CSV\n"
                mapping += "                ];\n"

                mapping += "    rr:subjectMap [\n"
                if "template" in triples_map.subject_map.subject_mapping_type:
                    mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
                elif "reference" in triples_map.subject_map.subject_mapping_type:
                    mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                elif "constant" in triples_map.subject_map.subject_mapping_type:
                    mapping += "        rr:constant \"" + triples_map.subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                if triples_map.subject_map.rdf_class[0] != None:
                    prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
                    mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ];\n"

                for predicate_object in triples_map.predicate_object_maps_list:
                    if predicate_object.predicate_map.value not in predicates and predicate_object.predicate_map.value not in general_predicates:
                        predicates[predicate_object.predicate_map.value] = ""
                    mapping += "    rr:predicateObjectMap [\n"
                    if "constant" in predicate_object.predicate_map.mapping_type :
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "constant shortcut" in predicate_object.predicate_map.mapping_type:
                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                    elif "template" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rr:template \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"
                    elif "reference" in predicate_object.predicate_map.mapping_type:
                        mapping += "        rr:predicateMap[\n"
                        mapping += "            rml:reference \"" + predicate_object.predicate_map.value + "\"\n"
                        mapping += "        ];\n"

                    mapping += "        rr:objectMap "
                    if "constant" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "template" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rr:template  \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "reference" == predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rml:reference \"" + predicate_object.object_map.value + "\"\n"
                        if predicate_object.object_map.datatype is not None:
                            prefix, url, value = prefix_extraction(original, predicate_object.object_map.datatype)
                            mapping = mapping[:-1]
                            mapping += ";\n            rr:datatype " + prefix + ":" + value + ";\n"
                        elif predicate_object.object_map.term is not None:
                            prefix, url, value = prefix_extraction(original, predicate_object.object_map.term)
                            mapping = mapping[:-1]
                            mapping += ";\n            rr:termType " + prefix + ":" + value + ";\n"
                        mapping += "        ]\n"
                    elif "constant shortcut" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "            rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "parent triples map" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">\n"
                        if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                            parent_triples_maps[predicate_object.object_map.value] = ""
                            mapping = mapping[:-1]
                            mapping += ";\n"
                            mapping += "        rr:joinCondition [\n"
                            mapping += "            rr:child \"" + predicate_object.object_map.child[0] + "\";\n"
                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[0] + "\";\n"
                            mapping += "            ]\n"
                        mapping += "        ]\n"
                    mapping += "    ];\n"

        mapping = mapping[:-2]
        mapping += ".\n\n"

    for tm in triples_map_list:
        if tm.triples_map_id in parent_triples_maps:
            mapping += "<" + tm.triples_map_id + ">\n"
            mapping += "    a rr:TriplesMap;\n"
            mapping += "    rml:logicalSource [ rml:source \"" + tm.data_source +"\";\n"
            mapping += "                rml:referenceFormulation ql:CSV\n"
            mapping += "            ];\n"
            mapping += "    rr:subjectMap [\n"
            mapping += "        rr:template \"" + tm.subject_map.value + "\";\n"
            if triples_map.subject_map.rdf_class[0] != None:
                    prefix, url, value = prefix_extraction(original, tm.subject_map.rdf_class[0])
                    mapping += "        rr:class " + prefix + ":" + value  + "\n"
            mapping += "    ].\n\n"

    prefix_string = ""

    f = open(original,"r")
    original_mapping = f.readlines()
    for prefix in original_mapping:
        if "prefix;" in prefix or "d2rq:Database;" in prefix:
            pass
        elif ("prefix" in prefix) or ("base" in prefix):
           prefix_string += prefix
    f.close()

    prefix_string += "\n"
    prefix_string += mapping
    mapping_file = open(output_folder + "/" + original.split("/")[len(original.split("/"))-1].split(".")[0] + "_submap_"+ str(number) +".ttl","w")
    mapping_file.write(prefix_string)
    mapping_file.close()
    return predicates

def prefix_extraction(original, uri):
    url = ""
    value = ""
    if prefixes:
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    else:
        f = open(original,"r")
        original_mapping = f.readlines()
        for prefix in original_mapping:
            if ("prefix" in prefix) or ("base" in prefix):
                elements = prefix.split(" ")
                elements[2] = elements[2].replace(" ","")
                elements[2] = elements[2].replace("\n","")
                if ">" in elements[2].replace(" ","")[1:-1]:
                    prefixes[elements[2].replace(" ","")[1:-2]] = elements[1][:-1]
                else:
                    prefixes[elements[2].replace(" ","")[1:-1]] = elements[1][:-1]
            else:
                break
        f.close()
        if "#" in uri:
            url, value = uri.split("#")[0]+"#", uri.split("#")[1]
        else:
            value = uri.split("/")[len(uri.split("/"))-1]
            char = ""
            temp = ""
            temp_string = uri
            while char != "/":
                temp = temp_string
                temp_string = temp_string[:-1]
                char = temp[len(temp)-1]
            url = temp
    return prefixes[url], url, value
