import re
import csv
import sys
import os

global prefixes
prefixes = {}

def join_grouping(triples_map_list):
    parent_child = {}
    for triples_map in triples_map_list:
        for predicate_object in triples_map.predicate_object_maps_list:
            if "parent triples map" in predicate_object.object_map.mapping_type:
                if None not in predicate_object.object_map.child and None not in predicate_object.object_map.parent :
                    for tp in triples_map_list:
                        if tp.triples_map_id == predicate_object.object_map.value:
                            if tp.triples_map_id not in parent_child:
                                parent_child[tp.triples_map_id] = [triples_map.triples_map_id]
                            else:
                                parent_child[tp.triples_map_id].append
    return parent_child

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

def update_mapping(original, output_folder, number, mapping_group, parent_child, triples_map_list):
    mapping = ""
    child_triples_maps = {}
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
                    if "parent triples map" not in predicate_object.object_map.mapping_type:
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
                        elif "constant shortcut" in predicate_object.object_map.mapping_type:
                            mapping += "[\n"
                            mapping += "        rr:constant \"" + predicate_object.object_map.value + "\"\n"
                            mapping += "        ]\n"
                        mapping += "    ];\n"
                    else:
                        if None in predicate_object.object_map.child and None in predicate_object.object_map.parent:
                            mapping += "    rr:predicateObjectMap [\n"
                            prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                            mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                            mapping += "    rr:objectMap [\n"
                            mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">\n"
                            mapping += "    ];\n"
                            mapping += "    ];\n"
                        else:
                            for tm in triples_map_list:
                                if tm.triples_map_id == predicate_object.object_map.value:
                                    if tm.data_source == triples_map.data_source:
                                        mapping += "    rr:predicateObjectMap [\n"
                                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                        mapping += "        rr:objectMap [\n"
                                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">\n"
                                        mapping = mapping[:-1]
                                        for i in range(len(predicate_object.object_map.child)):
                                            mapping += ";\n"
                                            mapping += "        rr:joinCondition [\n"
                                            mapping += "            rr:child \"" + predicate_object.object_map.child[i] + "\";\n"
                                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[i] + "\";\n"
                                            mapping += "        ]\n"
                                        mapping += "    ];\n"
        mapping = mapping[:-2]
        mapping += ".\n\n"

        for tm in mapping_group:
            if tm in parent_child:
                for child in parent_child[tm]:
                    for triples_map in triples_map_list:
                        if triples_map.triples_map_id not in child_triples_maps and triples_map.triples_map_id not in mapping_group and child == triples_map.triples_map_id:
                            mapping += "<" + triples_map.triples_map_id + ">\n"
                            mapping += "    a rr:TriplesMap;\n"
                            mapping += "    rml:logicalSource [ rml:source \"" + triples_map.data_source +"\";\n"
                            mapping += "                rml:referenceFormulation ql:CSV\n" 
                            mapping += "            ];\n"
                            mapping += "    rr:subjectMap [\n"
                            mapping += "        rr:template \"" + triples_map.subject_map.value + "\";\n"
                            mapping += "    ];\n\n"
                            for predicate_object in triples_map.predicate_object_maps_list:
                                if "parent triples map" in predicate_object.object_map.mapping_type:
                                    if predicate_object.object_map.value in mapping_group:
                                        mapping += "    rr:predicateObjectMap [\n"
                                        prefix, url, value = prefix_extraction(original, predicate_object.predicate_map.value)
                                        mapping += "        rr:predicate " + prefix + ":" + value + ";\n"
                                        mapping += "        rr:objectMap [\n"
                                        mapping += "        rr:parentTriplesMap <" + predicate_object.object_map.value + ">;\n"
                                        for i in range(len(predicate_object.object_map.child)):
                                            mapping += "        rr:joinCondition [\n"
                                            mapping += "            rr:child \"" + predicate_object.object_map.child[i] + "\";\n"
                                            mapping += "            rr:parent \"" + predicate_object.object_map.parent[i] + "\";\n"
                                            mapping += "        ];\n"
                                        mapping += "        ];\n"
                                        mapping += "    ];\n"
                            mapping = mapping[:-2]
                            mapping += ".\n\n" 
                            child_triples_maps[triples_map.triples_map_id] = ""

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
                elements[2] = elements[2].replace(" ","")
                elements[2] = elements[2].replace("\n","")
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