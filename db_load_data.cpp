#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include "pugixml.hpp" // Requires pugixml.hpp, pugiconfig.hpp and pugixml.cpp

using namespace std;
using namespace pugi;
namespace fs = filesystem;

// Represents the logical dimensional constraints
struct DimSchema {
    string type;
    bool not_null;
    bool unique;
    string attribute_type;
    unordered_set<string> seen_values;
};

// Represents the physical columnar files
struct ColumnSchema {
    string name;
    string file;
    unique_ptr<ofstream> out_stream;
    
    // Pointer to its corresponding logical schema constraints
    DimSchema* dim; 
};

// Simple CSV parser for lines (handles quoted commas like "Gift More, Spend Less")
vector<string> parse_csv_line(const string& line) {
    vector<string> result;
    string current_field;
    bool in_quotes = false;
    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        if (c == '"') {
            in_quotes = !in_quotes;
        } else if (c == ',' && !in_quotes) {
            result.push_back(current_field);
            current_field.clear();
        } else {
            current_field += c;
        }
    }
    result.push_back(current_field);
    return result;
}

int main() {
    string cds_path = "cds_schema.xml";
    string dim_path = "dim_schema.xml";
    string csv_path = "Data/fact_sales_denormalized_generated.csv";
    
    // 1. Load Dimensional Schema validations
    xml_document dim_doc;
    if (!dim_doc.load_file(dim_path.c_str())) {
        cerr << "Failed to parse " << dim_path << endl;
        return 1;
    }

    // Using unique_ptr to guarantee stable pointer addresses for DimSchema
    unordered_map<string, unique_ptr<DimSchema>> dim_map;
    xml_node dim_root = dim_doc.child("DimensionalSchema");
    
    for (xml_node col = dim_root.child("Column"); col; col = col.next_sibling("Column")) {
        string name = col.attribute("name").value();
        auto ds = make_unique<DimSchema>();
        ds->type = col.attribute("type").value();
        ds->not_null = string(col.attribute("not_null").value()) == "true";
        ds->unique = string(col.attribute("unique").value()) == "true";
        ds->attribute_type = col.attribute("attribute_type").value();
        
        dim_map[name] = std::move(ds);
    }
    
    cout << "Loaded " << dim_map.size() << " dimension properties for validation." << endl;

    // 2. Parse CDS Schema
    xml_document cds_doc;
    if (!cds_doc.load_file(cds_path.c_str())) {
        cerr << "Failed to parse " << cds_path << endl;
        return 1;
    }

    if (!fs::exists("DB")) {
        cerr << "DB directory does not exist! Please run db_init first to initialize the files." << endl;
        return 1;
    }

    vector<ColumnSchema> columns;
    xml_node table = cds_doc.child("ColumnStoreSchema").child("Table");
    if (!table) {
        cerr << "Failed to locate <Table> in CDS Schema" << endl;
        return 1;
    }

    // Combine CDS physical paths with Dimensional constraints
    for (xml_node colElement = table.child("Column"); colElement; colElement = colElement.next_sibling("Column")) {
        string name = colElement.attribute("name").value();
        string file = colElement.attribute("file").value();

        if (!name.empty() && !file.empty()) {
            if (dim_map.find(name) == dim_map.end()) {
                cerr << "Error: Column " << name << " in cds_schema.xml is missing from dim_schema.xml!" << endl;
                return 1;
            }
            
            ColumnSchema col;
            col.name = name;
            col.file = file;
            col.dim = dim_map[name].get(); // Link purely by pointer
            
            col.out_stream = make_unique<ofstream>(col.file, ios::app);
            if (!col.out_stream->is_open()) {
                cerr << "Failed to open output file for column " << col.name << " at " << col.file << endl;
                return 1;
            }
            columns.push_back(std::move(col));
        }
    }

    cout << "Opened " << columns.size() << " binary files for appending." << endl;

    // 3. Process CSV Data
    cout << "Opening CSV data file: " << csv_path << endl;
    ifstream csv_file(csv_path);
    if (!csv_file.is_open()) {
        cerr << "Failed to open CSV " << csv_path << endl;
        return 1;
    }

    string csv_line;
    // Skip header line
    if (!getline(csv_file, csv_line)) {
        cerr << "Empty CSV file!" << endl;
        return 1;
    }

    size_t row_count = 0;
    size_t validation_errors = 0;

    // Line by Line Evaluation
    while (getline(csv_file, csv_line)) {
        if (csv_line.empty()) continue;
        
        vector<string> fields = parse_csv_line(csv_line);
        if (fields.size() != columns.size()) {
            cerr << "Row " << row_count + 1 << " column count mismatch. Skipping." << endl;
            continue;
        }

        // === VALIDATION PASS ===
        bool valid = true;
        for (size_t i = 0; i < columns.size(); ++i) {
            auto& col = columns[i];
            auto& field = fields[i];
            
            // 1. Not Null Validation
            if (col.dim->not_null && field.empty()) {
                cerr << "Validation Error (Row " << row_count + 1 << "): Column '" << col.name << "' cannot be null." << endl;
                valid = false;
                break;
            }
            
            // 2. Uniqueness Validation
            if (col.dim->unique) {
                if (col.dim->seen_values.find(field) != col.dim->seen_values.end()) {
                    cerr << "Validation Error (Row " << row_count + 1 << "): Column '" << col.name 
                         << "' requires unique values. Duplicate: " << field << endl;
                    valid = false;
                    break;
                }
            }
        }
        
        if (!valid) {
            validation_errors++;
            continue; // Skip appending this row
        }

        // === PERSIST COMPLIANT DATA ===
        for (size_t i = 0; i < columns.size(); ++i) {
            auto& col = columns[i];
            auto& field = fields[i];
            auto& out = *(col.out_stream);
            
            // Record uniqueness state
            if (col.dim->unique) {
                col.dim->seen_values.insert(field);
            }

            // Write text mappings using DimSchema datatypes
            if (col.dim->type == "integer") {
                int32_t val = 0;
                if (!field.empty()) {
                    try { val = stoi(field); } catch(...) {}
                }
                out << val << "\n";
            } else if (col.dim->type == "float") {
                float val = 0.0f;
                if (!field.empty()) {
                    try { val = stof(field); } catch(...) {}
                }
                out << val << "\n";
            } else if (col.dim->type == "string") {
                out << field << "\n";
            }
        }
        row_count++;
        
        // Console Progress
        if (row_count % 100000 == 0) {
            cout << "Appended " << row_count << " valid rows..." << endl;
        }
    }

    cout << "Finished! Successfully appended " << row_count << " rows." << endl;
    if (validation_errors > 0) {
        cout << "Encountered " << validation_errors << " validation errors that were skipped." << endl;
    }
    return 0;
}
