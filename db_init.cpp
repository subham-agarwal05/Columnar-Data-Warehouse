#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include "pugixml.hpp" // Requires pugixml.hpp, pugiconfig.hpp and pugixml.cpp

using namespace std;
using namespace pugi;
namespace fs = filesystem;

int main() {
    string schema_path = "cds_schema.xml";
    
    // 1. Parse Schema using pugixml
    xml_document doc;
    xml_parse_result result = doc.load_file(schema_path.c_str());
    if (!result) {
        cerr << "Failed to parse " << schema_path << ". Error: " << result.description() << endl;
        return 1;
    }

    // Ensure DB directory exists
    if (!fs::exists("DB")) {
        fs::create_directory("DB");
    }

    // Get root element <ColumnStoreSchema>
    xml_node root = doc.child("ColumnStoreSchema");
    if (!root) {
        cerr << "No root element <ColumnStoreSchema> in XML." << endl;
        return 1;
    }

    // Get <Table> element
    xml_node table = root.child("Table");
    if (!table) {
        cerr << "No <Table> element found." << endl;
        return 1;
    }

    // Iterate through <Column> elements, creating the empty files
    int files_created = 0;
    for (xml_node colElement = table.child("Column"); colElement; colElement = colElement.next_sibling("Column")) {
        string file = colElement.attribute("file").value();
        string dict_file = colElement.attribute("dict").value();

        if (!file.empty()) {
            // Create empty column file
            ofstream out_stream(file, ios::binary | ios::trunc); 
            
            if (!out_stream.is_open()) {
                cerr << "Failed to initialize and open output file " << file << endl;
                return 1;
            }
            out_stream.close();
            files_created++;
        }

        if (!dict_file.empty()) {
            // Create empty dictionary file
            ofstream dict_stream(dict_file, ios::binary | ios::trunc);
            if (!dict_stream.is_open()) {
                cerr << "Failed to initialize dictionary file " << dict_file << endl;
                return 1;
            }
            dict_stream.close();
            files_created++;
        }
    }

    cout << "Database successfully initialized! Created " << files_created << " empty binary files." << endl;
    return 0;
}
