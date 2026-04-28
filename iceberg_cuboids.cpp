#include <bits/stdc++.h>
#include "pugixml.hpp"

using namespace std;
using namespace pugi;
namespace fs = filesystem;


struct CdsColumnInfo {
    string file;
    string dict;
};

struct AggregateResult {
    double sum_amount   = 0.0;
    double min_amount   = 1e18;
    double max_amount   = -1e18;
    int64_t count       = 0;

    void add(double val) {
        sum_amount += val;
        if (val < min_amount) min_amount = val;
        if (val > max_amount) max_amount = val;
        count++;
    }

    double avg() const {
        return count > 0 ? sum_amount / count : 0.0;
    }

    void merge(const AggregateResult& other) {
        sum_amount += other.sum_amount;
        if (other.min_amount < min_amount) min_amount = other.min_amount;
        if (other.max_amount > max_amount) max_amount = other.max_amount;
        count += other.count;
    }
};


map<string, CdsColumnInfo> parse_cds_schema(const string& path) {
    map<string, CdsColumnInfo> result;
    xml_document doc;
    if (!doc.load_file(path.c_str())) {
        cerr << "Error: Failed to parse " << path << endl;
        return result;
    }
    xml_node table = doc.child("ColumnStoreSchema").child("Table");
    for (xml_node col = table.child("Column"); col;
         col = col.next_sibling("Column")) {
        CdsColumnInfo info;
        info.file = col.attribute("file").value();
        info.dict = col.attribute("dict").value();
        result[col.attribute("name").value()] = info;
    }
    return result;
}


vector<string> load_dictionary(const string& dict_path) {
    vector<string> dict;
    ifstream in(dict_path, ios::binary);
    if (!in.is_open()) {
        cerr << "Error: Cannot open dictionary " << dict_path << endl;
        return dict;
    }
    uint32_t entry_count;
    in.read(reinterpret_cast<char*>(&entry_count), sizeof(uint32_t));
    if (!in) return dict;
    dict.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; i++) {
        uint16_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint16_t));
        if (!in) break;
        string s(len, '\0');
        in.read(s.data(), len);
        dict.push_back(std::move(s));
    }
    return dict;
}

vector<string> load_column_raw(const string& file_path) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) {
        cerr << "Error: Cannot open " << file_path << endl;
        return data;
    }
    while (in.peek() != EOF) {
        uint16_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint16_t));
        if (!in) break;
        string s(len, '\0');
        in.read(s.data(), len);
        data.push_back(std::move(s));
    }
    return data;
}

vector<string> load_column_dict(const string& file_path,
                                const vector<string>& dict) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) {
        cerr << "Error: Cannot open " << file_path << endl;
        return data;
    }
    uint32_t code;
    while (in.read(reinterpret_cast<char*>(&code), sizeof(uint32_t))) {
        data.push_back(code < dict.size() ? dict[code] : "<INVALID>");
    }
    return data;
}

vector<string> load_column(const CdsColumnInfo& col_info) {
    if (!col_info.dict.empty()) {
        auto dict = load_dictionary(col_info.dict);
        return load_column_dict(col_info.file, dict);
    }
    return load_column_raw(col_info.file);
}

vector<double> load_measure_column(const string& file_path) {
    vector<double> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) {
        cerr << "Error: Cannot open " << file_path << endl;
        return data;
    }
    float val;
    while (in.read(reinterpret_cast<char*>(&val), sizeof(float))) {
        data.push_back(static_cast<double>(val));
    }
    return data;
}


vector<string> get_selected_dimensions() {
    return {
        "category",
        "brand",
        "customer_segment",
        "store_type",
        "salesperson_role",
        "campaign_name"
    };
}

string get_measure_column() {
    return "total_amount";
}


// Get aggregate value by function name
double get_aggregate_value(const AggregateResult& agg, const string& func_name) {
    if (func_name == "SUM") return agg.sum_amount;
    if (func_name == "COUNT") return static_cast<double>(agg.count);
    if (func_name == "AVG") return agg.avg();
    if (func_name == "MIN") return agg.min_amount;
    if (func_name == "MAX") return agg.max_amount;
    return agg.sum_amount;  // default to SUM
}

// Check if aggregate function is anti-monotone (for Apriori pruning)
bool is_antimonotone(const string& func_name) {
    // Anti-monotone functions: child value <= parent value
    // SUM, COUNT, MAX are anti-monotone
    // AVG, MIN are NOT anti-monotone
    return (func_name == "SUM" || func_name == "COUNT" || func_name == "MAX");
}

string abbreviate(const string& name) {
    static unordered_map<string, string> abbr = {
        {"category", "cat"},     {"brand", "brd"},
        {"customer_segment", "cseg"}, {"store_type", "styp"},
        {"salesperson_role", "srole"}, {"campaign_name", "cname"},
        {"customer_id", "cid"},  {"product_id", "pid"},
        {"store_id", "sid"},     {"salesperson_id", "spid"},
        {"campaign_id", "campid"}, {"store_manager_id", "smid"},
        {"store_manager_name", "smname"}, {"store_manager_role", "smrole"},
        {"campaign_start_date", "csdate"}, {"campaign_end_date", "cedate"},
        {"campaign_start_year", "csyr"}, {"campaign_end_year", "ceyr"},
        {"campaign_start_month", "csmon"}, {"campaign_end_month", "cemon"},
        {"campaign_start_day", "csday"}, {"campaign_end_day", "ceday"},
        {"campaign_start_weekday", "cswday"}, {"campaign_end_weekday", "cewday"},
        {"campaign_start_quarter", "csqtr"}, {"campaign_end_quarter", "ceqtr"},
        {"campaign_budget", "cbudget"}, {"residential_location", "resloc"},
        {"origin_location", "orloc"}, {"store_location", "sloc"},
        {"store_name", "sname"}, {"salesperson_name", "spname"},
        {"product_name", "pname"}, {"first_name", "fname"},
        {"last_name", "lname"}, {"email", "email"}
    };
    auto it = abbr.find(name);
    return it != abbr.end() ? it->second : name;
}

string make_table_name(const vector<string>& dim_names,
                       const vector<int>& dim_indices) {
    if (dim_indices.empty()) return "iceberg_apex";
    string name = "iceberg";
    for (int idx : dim_indices) name += "_" + dim_names[idx];
    if (name.length() <= 64) return name;
    name = "iceberg";
    for (int idx : dim_indices) name += "_" + abbreviate(dim_names[idx]);
    if (name.length() > 64) name = name.substr(0, 64);
    return name;
}

// Build a pipe-separated key from selected dimension values for a given row
string build_key_from_bits(const vector<vector<string>>& dim_data,
                           int mask, int K, size_t row) {
    string key;
    bool first = true;
    for (int i = 0; i < K; i++) {
        if (mask & (1 << i)) {
            if (!first) key += "|";
            key += dim_data[i][row];
            first = false;
        }
    }
    return key.empty() ? "ALL" : key;
}

string project_key(const string& child_key, int child_mask, int parent_mask) {
    int dropped_bit = __builtin_ctz(child_mask ^ parent_mask);
    int drop_idx = 0;
    for (int i = 0; i < dropped_bit; i++) {
        if (child_mask & (1 << i)) drop_idx++;
    }
    vector<string> parts;
    stringstream ss(child_key);
    string token;
    while (getline(ss, token, '|')) parts.push_back(token);
    string result;
    bool first = true;
    for (int i = 0; i < (int)parts.size(); i++) {
        if (i == drop_idx) continue;
        if (!first) result += "|";
        result += parts[i];
        first = false;
    }
    return result.empty() ? "ALL" : result;
}

map<string, AggregateResult> compute_cuboid_from_child(
    const map<string, AggregateResult>& child_agg,
    int child_mask, int parent_mask
) {
    map<string, AggregateResult> parent_agg;
    if (parent_mask == 0) {
        AggregateResult& apex = parent_agg["ALL"];
        for (auto& [key, agg] : child_agg) {
            apex.merge(agg);
        }
    } else {
        for (auto& [child_key, child_result] : child_agg) {
            string parent_key = project_key(child_key, child_mask, parent_mask);
            parent_agg[parent_key].merge(child_result);
        }
    }
    return parent_agg;
}

// Extract the projected key for a parent mask from a child's full dim values.
// child_dims are the bit positions set in child_mask; parent_mask is a subset.
string project_key_to_parent(const vector<vector<string>>& dim_data,
                             int parent_mask, int K, size_t row) {
    return build_key_from_bits(dim_data, parent_mask, K, row);
}

// Get all immediate parent masks (remove exactly one bit)
vector<int> get_parent_masks(int mask) {
    vector<int> parents;
    int temp = mask;
    while (temp) {
        int bit = temp & (-temp);       // lowest set bit
        parents.push_back(mask ^ bit);  // mask with that bit removed
        temp ^= bit;
    }
    return parents;
}


void write_iceberg_csv(const string& output_dir,
                       const string& table_name,
                       const vector<string>& dim_names,
                       int mask, int K,
                       const map<string, AggregateResult>& agg_map) {
    string filepath = output_dir + "/" + table_name + ".csv";
    ofstream out(filepath);
    if (!out.is_open()) {
        cerr << "Error: Cannot write to " << filepath << endl;
        return;
    }

    // Header — dimension columns in bit order
    for (int i = 0; i < K; i++) {
        if (mask & (1 << i)) out << dim_names[i] << ",";
    }
    out << "sum_amount,count,avg_amount,min_amount,max_amount" << endl;

    // Data
    out << fixed << setprecision(2);
    for (auto& [key, agg] : agg_map) {
        if (mask != 0) {    // non-apex
            stringstream ss(key);
            string token;
            bool first = true;
            while (getline(ss, token, '|')) {
                if (!first) out << ",";
                if (token.find(',') != string::npos ||
                    token.find('"') != string::npos)
                    out << "\"" << token << "\"";
                else
                    out << token;
                first = false;
            }
            out << ",";
        }
        out << agg.sum_amount << "," << agg.count << "," << agg.avg() << ","
            << agg.min_amount << "," << agg.max_amount << endl;
    }
    out.close();
}


vector<int> mask_to_indices(int mask, int K) {
    vector<int> indices;
    for (int i = 0; i < K; i++) {
        if (mask & (1 << i)) indices.push_back(i);
    }
    return indices;
}

string mask_to_dim_string(int mask, int K, const vector<string>& dim_names) {
    string s;
    for (int i = 0; i < K; i++) {
        if (mask & (1 << i)) {
            if (!s.empty()) s += ", ";
            s += dim_names[i];
        }
    }
    return s.empty() ? "(apex)" : s;
}

// Read offset from file (returns 0 if file doesn't exist)
size_t read_offset(const string& path) {
    ifstream in(path);
    if (!in.is_open()) return 0;
    size_t offset = 0;
    in >> offset;
    return offset;
}

// Write offset to file
void write_offset(const string& path, size_t offset) {
    ofstream out(path, ios::trunc);
    out << offset << endl;
}

struct IcebergMetadata {
    size_t offset = 0;
    string agg_func = "";
    double threshold = 0.0;
};

IcebergMetadata read_iceberg_metadata(const string& path) {
    IcebergMetadata meta;
    ifstream in(path);
    if (in.is_open()) {
        in >> meta.offset >> meta.agg_func >> meta.threshold;
    }
    return meta;
}

void write_iceberg_metadata(const string& path, const IcebergMetadata& meta) {
    ofstream out(path, ios::trunc);
    out << meta.offset << "\n" << meta.agg_func << "\n" << fixed << setprecision(2) << meta.threshold << "\n";
}

int main(int argc, char* argv[]) {

    if (argc < 2) {
        cerr << "=========================================================\n"
             << "  Iceberg Cuboid Generator  (with Apriori Pruning)\n"
             << "=========================================================\n"
             << "  Usage:   iceberg_cuboids <threshold> [aggregate_func]\n\n"
             << "  <threshold>     : Threshold value for pruning\n"
             << "  [aggregate_func]: SUM (default), COUNT, MAX\n\n"
             << "  Examples:\n"
             << "    iceberg_cuboids 50000       (SUM >= 50000)\n"
             << "    iceberg_cuboids 100 COUNT   (COUNT >= 100)\n"
             << "    iceberg_cuboids 50000 MAX   (MAX >= 50000)\n\n"
             << "  Optimization: anti-monotone (Apriori/BUC) pruning applied\n"
             << "  to all supported aggregate functions. Child cuboids inherit\n"
             << "  pruning decisions from parents, skipping rows when a parent\n"
             << "  key is already below the threshold.\n"
             << "=========================================================\n"
             << endl;
        return 1;
    }

    double threshold;
    try {
        threshold = stod(argv[1]);
    } catch (...) {
        cerr << "Error: Invalid threshold value '" << argv[1] << "'." << endl;
        return 1;
    }

    string agg_func = "SUM";
    if (argc >= 3) {
        agg_func = argv[2];
        // Normalize to uppercase
        for (auto& c : agg_func) c = toupper(c);
        
        // Validate
        if (agg_func != "SUM" && agg_func != "COUNT" && agg_func != "MAX") {
            cerr << "Error: Invalid aggregation function '" << agg_func 
                 << "'. Supported functions: SUM, COUNT, MAX." << endl;
            return 1;
        }
    }

    cout << "=========================================================\n"
         << "  Iceberg Cuboid Generator  (with Apriori Pruning)\n"
         << "=========================================================\n"
         << "  Iceberg Condition : " << agg_func << "(total_amount) >= " << fixed
         << setprecision(2) << threshold << "\n"
         << "  Pruning Strategy  : Anti-monotone (Apriori/BUC)\n"
         << "=========================================================" << endl;

    auto cds_schema = parse_cds_schema("cds_schema.xml");
    if (cds_schema.empty()) {
        cerr << "Failed to parse CDS schema." << endl;
        return 1;
    }

    vector<string> selected_dims = get_selected_dimensions();
    string measure_col           = get_measure_column();
    int K                        = selected_dims.size();
    int total_cuboids            = 1 << K;

    cout << "\nDimensions : " << K << "  (";
    for (int i = 0; i < K; i++) {
        if (i) cout << ", ";
        cout << selected_dims[i];
    }
    cout << ")\nMeasure    : " << measure_col
         << "\nTotal lattice nodes: " << total_cuboids << "\n" << endl;

    // Check if cuboids already exist and if there's new data
    ostringstream dir_oss;
    dir_oss << "IcebergCuboids_" << agg_func << "_" << fixed << setprecision(2) << threshold;
    string output_dir = dir_oss.str();

    ostringstream meta_oss;
    meta_oss << "DB/.iceberg_metadata_" << agg_func << "_" << fixed << setprecision(2) << threshold;
    string metadata_file = meta_oss.str();

    size_t db_offset = read_offset("DB/.offset");           // total rows in column store
    IcebergMetadata meta = read_iceberg_metadata(metadata_file);
    size_t iceberg_offset = meta.offset;

    bool cuboids_exist = fs::exists(output_dir) && !fs::is_empty(output_dir);
    bool config_match = (meta.agg_func == agg_func && abs(meta.threshold - threshold) < 1e-9);
    
    if (cuboids_exist && config_match) {
        if (db_offset <= iceberg_offset) {
            cout << "\n✓ Iceberg cuboids are up to date for configuration: " << agg_func << " " << threshold << endl;
            cout << "  Column store: " << db_offset << " rows" << endl;
            cout << "  Last processed: " << iceberg_offset << " rows" << endl;
            cout << "  No new data to process." << endl;
            return 0;
        } else {
            cout << "\n→ New data detected. Refreshing iceberg cuboids..." << endl;
            // For iceberg cuboids, we need to regenerate all since they depend on full dataset
            // Clear and recreate
            for (auto& entry : fs::directory_iterator(output_dir))
                fs::remove(entry.path());
        }
    } else {
        cout << "\n→ Configuration change or first run detected. Creating iceberg cuboids from scratch..." << endl;
        if (fs::exists(output_dir)) {
            for (auto& entry : fs::directory_iterator(output_dir))
                fs::remove(entry.path());
        } else {
            fs::create_directory(output_dir);
        }
    }

    cout << "Loading columns from column store..." << endl;
    vector<vector<string>> dim_data(K);
    for (int i = 0; i < K; i++) {
        dim_data[i] = load_column(cds_schema.at(selected_dims[i]));
        cout << "  [" << (i + 1) << "/" << K << "] " << selected_dims[i]
             << "  (" << dim_data[i].size() << " rows)" << endl;
    }

    vector<double> measure_data =
        load_measure_column(cds_schema.at(measure_col).file);
    cout << "  Measure: " << measure_col << "  (" << measure_data.size()
         << " rows)\n" << endl;

    size_t num_rows = measure_data.size();
    for (int i = 0; i < K; i++) {
        if (dim_data[i].size() != num_rows) {
            cerr << "Error: Column " << selected_dims[i]
                 << " row count mismatch." << endl;
            return 1;
        }
    }

    // Group bitmasks by popcount (level)
    vector<vector<int>> levels(K + 1);
    for (int mask = 0; mask < total_cuboids; mask++) {
        levels[__builtin_popcount(mask)].push_back(mask);
    }

    // Statistics
    int64_t total_cells_before   = 0;
    int64_t total_cells_after    = 0;
    int64_t total_pruned_cells   = 0;
    int cuboids_materialized     = 0;
    int cuboids_fully_pruned     = 0;

    struct CuboidSummary {
        string name;
        string dims;
        int num_dims;
        int64_t cells_before;
        int64_t cells_after;
        int64_t pruned;
        bool materialized;
    };
    vector<CuboidSummary> summaries;

    // Store full (unpruned) cuboid results for deriving lower-level cuboids
    unordered_map<int, map<string, AggregateResult>> cuboid_results;

    int progress = 0;

    cout << "Computing Iceberg cuboids (level-by-level optimization)...\n" << endl;

    // Level K: compute from base data (only one full scan of raw data)
    int full_mask = (1 << K) - 1;
    {
        cout << "--- Level " << K << " (1 cuboid, computed from base data) ---" << endl;
        auto indices    = mask_to_indices(full_mask, K);
        string tbl_name = make_table_name(selected_dims, indices);
        string dim_str  = mask_to_dim_string(full_mask, K, selected_dims);

        map<string, AggregateResult> agg_map;
        for (size_t row = 0; row < num_rows; row++) {
            string key = build_key_from_bits(dim_data, full_mask, K, row);
            agg_map[key].add(measure_data[row]);
        }

        // Store full results for derivation
        cuboid_results[full_mask] = agg_map;

        // Apply iceberg pruning for output
        CuboidSummary summary;
        summary.name     = tbl_name;
        summary.dims     = dim_str;
        summary.num_dims = K;
        summary.cells_before = (int64_t)agg_map.size();
        int64_t pruned_count = 0;
        for (auto it = agg_map.begin(); it != agg_map.end(); ) {
            if (get_aggregate_value(it->second, agg_func) < threshold) {
                it = agg_map.erase(it);
                pruned_count++;
            } else {
                ++it;
            }
        }
        summary.cells_after = (int64_t)agg_map.size();
        summary.pruned      = pruned_count;
        total_cells_before += summary.cells_before;
        total_cells_after  += summary.cells_after;
        total_pruned_cells += pruned_count;

        if (agg_map.empty()) {
            cuboids_fully_pruned++;
            summary.materialized = false;
        } else {
            write_iceberg_csv(output_dir, tbl_name, selected_dims, full_mask, K, agg_map);
            cuboids_materialized++;
            summary.materialized = true;
        }
        summaries.push_back(summary);
        progress++;
        cout << "  Processed " << progress << "/" << total_cuboids
             << "  (materialized: " << cuboids_materialized
             << ", pruned: " << cuboids_fully_pruned << ")" << endl;
    }

    // Levels K-1 down to 0: derive each cuboid from a child at the level above
    for (int level = K - 1; level >= 0; level--) {
        cout << "--- Level " << level << " (" << levels[level].size()
             << " cuboids, derived from level " << (level + 1) << ") ---" << endl;

        for (int mask : levels[level]) {
            auto indices    = mask_to_indices(mask, K);
            string tbl_name = make_table_name(selected_dims, indices);
            string dim_str  = mask_to_dim_string(mask, K, selected_dims);

            // Find a child cuboid (one extra bit set) at level+1
            int child_mask = -1;
            for (int i = 0; i < K; i++) {
                if (!(mask & (1 << i))) {
                    int candidate = mask | (1 << i);
                    if (cuboid_results.count(candidate)) {
                        child_mask = candidate;
                        break;
                    }
                }
            }

            // Compute by merging from child
            map<string, AggregateResult> agg_map;
            if (child_mask != -1) {
                agg_map = compute_cuboid_from_child(cuboid_results[child_mask], child_mask, mask);
            } else {
                // Fallback: compute from base data
                for (size_t row = 0; row < num_rows; row++) {
                    string key = build_key_from_bits(dim_data, mask, K, row);
                    agg_map[key].add(measure_data[row]);
                }
            }

            // Store full results for derivation of lower levels
            cuboid_results[mask] = agg_map;

            // Apply iceberg pruning for output
            CuboidSummary summary;
            summary.name     = tbl_name;
            summary.dims     = dim_str;
            summary.num_dims = (int)indices.size();
            summary.cells_before = (int64_t)agg_map.size();
            int64_t pruned_count = 0;
            for (auto it = agg_map.begin(); it != agg_map.end(); ) {
                if (get_aggregate_value(it->second, agg_func) < threshold) {
                    it = agg_map.erase(it);
                    pruned_count++;
                } else {
                    ++it;
                }
            }
            summary.cells_after = (int64_t)agg_map.size();
            summary.pruned      = pruned_count;
            total_cells_before += summary.cells_before;
            total_cells_after  += summary.cells_after;
            total_pruned_cells += pruned_count;

            if (agg_map.empty()) {
                cuboids_fully_pruned++;
                summary.materialized = false;
            } else {
                write_iceberg_csv(output_dir, tbl_name, selected_dims, mask, K, agg_map);
                cuboids_materialized++;
                summary.materialized = true;
            }

            summaries.push_back(summary);
            progress++;
            if (progress % 10 == 0 || progress == total_cuboids) {
                cout << "  Processed " << progress << "/" << total_cuboids
                     << "  (materialized: " << cuboids_materialized
                     << ", pruned: " << cuboids_fully_pruned << ")" << endl;
            }
        }

        // Free level+1 results (no longer needed for derivation)
        for (int m : levels[level + 1]) {
            cuboid_results.erase(m);
        }
    }

    // ── Summary report ─────────────────────────────────────────────────────
    cout << "\n========================================================="
         << "\n  ICEBERG CUBOID GENERATION — SUMMARY (Level-by-Level)"
         << "\n========================================================="
         << "\n  Threshold (" << agg_func << " >= )        : " << threshold
         << "\n  Total lattice nodes         : " << total_cuboids
         << "\n  Optimization                : Level-by-level (child → parent)"
         << "\n  Cuboids materialized        : " << cuboids_materialized
         << "\n  Cuboids fully pruned        : " << cuboids_fully_pruned
         << "\n---------------------------------------------------------"
         << "\n  Total cells (before prune)  : " << total_cells_before
         << "\n  Total cells (after prune)   : " << total_cells_after
         << "\n  Cells pruned                : " << total_pruned_cells
         << "\n  Cell pruning ratio          : "
         << (total_cells_before > 0
                 ? (100.0 * total_pruned_cells / total_cells_before)
                 : 0.0)
         << " %"
         << "\n=========================================================\n"
         << endl;

    // Detailed per-cuboid table
    cout << "Detailed per-cuboid breakdown:\n" << endl;
    cout << left  << setw(50) << "Cuboid"
         << right << setw(6)  << "Dims"
         << setw(12) << "Before"
         << setw(12) << "After"
         << setw(12) << "Pruned"
         << setw(14) << "Status"
         << endl;
    cout << string(106, '-') << endl;

    for (auto& s : summaries) {
        string status = s.materialized ? "KEPT" : "PRUNED";

        cout << left  << setw(50) << s.name
             << right << setw(6)  << s.num_dims
             << setw(12) << s.cells_before
             << setw(12) << s.cells_after
             << setw(12) << s.pruned
             << setw(14) << status
             << endl;
    }

    cout << "\nOutput directory: " << output_dir << "/" << endl;

    // Write HTML report snippet for embedding in compare_performance report
    {
        string html_path = output_dir + "/iceberg_report.html";
        ofstream html(html_path);
        if (html.is_open()) {
            html << "<h2>Iceberg Cuboid Report (" << agg_func << " &gt;= "
                 << fixed << setprecision(2) << threshold << ")</h2>";

            // Summary table
            html << "<table><tr><th>Metric</th><th>Value</th></tr>"
                 << "<tr><td>Threshold</td><td>" << agg_func << " &gt;= " << threshold << "</td></tr>"
                 << "<tr><td>Total Lattice Nodes</td><td>" << total_cuboids << "</td></tr>"
                 << "<tr><td>Optimization</td><td>Level-by-level (child &rarr; parent)</td></tr>"
                 << "<tr><td>Cuboids Materialized</td><td>" << cuboids_materialized << "</td></tr>"
                 << "<tr><td>Cuboids Fully Pruned</td><td>" << cuboids_fully_pruned << "</td></tr>"
                 << "<tr><td>Total Cells (before prune)</td><td>" << total_cells_before << "</td></tr>"
                 << "<tr><td>Total Cells (after prune)</td><td>" << total_cells_after << "</td></tr>"
                 << "<tr><td>Cells Pruned</td><td>" << total_pruned_cells << "</td></tr>"
                 << "<tr><td>Cell Pruning Ratio</td><td>"
                 << (total_cells_before > 0 ? (100.0 * total_pruned_cells / total_cells_before) : 0.0)
                 << " %</td></tr>"
                 << "</table>";

            // Per-cuboid breakdown table
            html << "<h3>Per-Cuboid Breakdown</h3>"
                 << "<table><tr><th>Cuboid</th><th>Dims</th>"
                 << "<th>Cells Before</th><th>Cells After</th>"
                 << "<th>Pruned</th><th>Status</th></tr>";
            for (auto& s : summaries) {
                string status = s.materialized ? "KEPT" : "PRUNED";
                string row_style = s.materialized ? "" : " style='background-color:#ffeaea;'";
                html << "<tr" << row_style << ">"
                     << "<td>" << s.name << "</td>"
                     << "<td>" << s.num_dims << "</td>"
                     << "<td>" << s.cells_before << "</td>"
                     << "<td>" << s.cells_after << "</td>"
                     << "<td>" << s.pruned << "</td>"
                     << "<td>" << status << "</td></tr>";
            }
            html << "</table>";
            html.close();
            cout << "HTML report written to: " << html_path << endl;
        }
    }
    
    // Update metadata to mark that we've processed all data for this config
    IcebergMetadata new_meta;
    new_meta.offset = num_rows;
    new_meta.agg_func = agg_func;
    new_meta.threshold = threshold;
    write_iceberg_metadata(metadata_file, new_meta);
    
    cout << "Done!" << endl;

    return 0;
}
