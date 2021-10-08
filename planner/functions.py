import re
import csv
import sys
import os

global prefixes
prefixes = {}

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
    for tm in mapping_group:
        for triples_map in triples_map_list:
            if tm == triples_map.triples_map_id:
                if "#" in triples_map.triples_map_id:
                    mapping += "<" + triples_map.triples_map_id.split("#")[1] + ">\n"
                else: 
                    mapping += "<" + triples_map.triples_map_id + ">\n"
                mapping += "    a rr:TriplesMap;\n"
                mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
                if str(triples_map.file_format).lower() == "csv" and triples_map.query == "None": 
                    mapping += "                rml:referenceFormulation ql:CSV\n" 
                mapping += "                ];\n"

                mapping += "    rr:subjectMap [\n"
                if triples_map.subject_map.subject_mapping_type is "template":
                    mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
                elif triples_map.subject_map.subject_mapping_type is "reference":
                    mapping += "        rml:reference \"" + triples_map.subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                elif triples_map.subject_map.subject_mapping_type is "constant":
                    mapping += "        rr:constant \"" + triples_map.subject_map.value + "\";\n"
                    mapping += "        rr:termType rr:IRI\n"
                if triples_map.subject_map.rdf_class[0] is not None:
                    prefix, url, value = prefix_extraction(original, triples_map.subject_map.rdf_class[0])
                    mapping += "        rr:class " + prefix + ":" + value  + "\n"
                mapping += "    ];\n"

                for predicate_object in triples_map.predicate_object_maps_list:
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
                        mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "template" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:template  \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "reference" == predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rml:reference \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    elif "parent triples map" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">\n"
                        if (predicate_object.object_map.child is not None) and (predicate_object.object_map.parent is not None):
                            if predicate_object.object_map.value not in parent_triples_maps:
                                parent_triples_maps[predicate_object.object_map.value] = ""
                            mapping = mapping[:-1]
                            for i in range(len(predicate_object.object_map.child)):
                                mapping += ";\n"
                                mapping += "        rr:joinCondition [\n"
                                mapping += "            rr:child \"" + predicate_object.object_map.child[i] + "\";\n"
                                mapping += "            rr:parent \"" + predicate_object.object_map.parent[i] + "\";\n"
                                mapping += "        ]\n"
                        mapping += "        ]\n"
                    elif "constant shortcut" in predicate_object.object_map.mapping_type:
                        mapping += "[\n"
                        mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                        mapping += "        ]\n"
                    mapping += "    ];\n"
        mapping = mapping[:-2]
        mapping += ".\n\n"

        if parent_triples_maps:
            for triples_map in triples_map_list:
                if triples_map.triples_map_id in parent_triples_maps and triples_map.triples_map_id not in mapping_group:
                    mapping += "<" + triples_map.triples_map_id + ">\n"
                    mapping += "    a rr:TriplesMap;\n"
                    mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
                    mapping += "                rml:referenceFormulation ql:CSV\n" 
                    mapping += "            ];\n"
                    mapping += "    rr:subjectMap [\n"
                    mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
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
               prefixes[elements[2][1:-3]] = elements[1][:-1]
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