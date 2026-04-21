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


int main(int argc, char* argv[]) {

    if (argc < 2) {
        cerr << "=========================================================\n"
             << "  Iceberg Cuboid Generator  (with Apriori Pruning)\n"
             << "=========================================================\n"
             << "  Usage:   iceberg_cuboids.exe <min_sales_threshold>\n\n"
             << "  Example: iceberg_cuboids.exe 50000\n"
             << "           Only cells with SUM(total_amount) >= 50000\n"
             << "           will be materialized.\n\n"
             << "  Optimization: anti-monotone (Apriori) pruning — child\n"
             << "  cuboids inherit pruning decisions from parents, so\n"
             << "  entire rows are skipped when a parent key is already\n"
             << "  below the threshold.\n"
             << "========================================================="
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

    cout << "=========================================================\n"
         << "  Iceberg Cuboid Generator  (with Apriori Pruning)\n"
         << "=========================================================\n"
         << "  Iceberg Condition : SUM(total_amount) >= " << fixed
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

    //Prepare output directory
    string output_dir = "IcebergCuboids";
    if (fs::exists(output_dir)) {
        for (auto& entry : fs::directory_iterator(output_dir))
            fs::remove(entry.path());
    } else {
        fs::create_directory(output_dir);
    }

    // Group bitmasks by popcount (level)
    vector<vector<int>> levels(K + 1);
    for (int mask = 0; mask < total_cuboids; mask++) {
        levels[__builtin_popcount(mask)].push_back(mask);
    }

    // Pruned keys per cuboid (bitmask → set of pruned key strings)
    unordered_map<int, unordered_set<string>> pruned_keys;

    // Cuboids where ALL cells were pruned (zero survivors)
    unordered_set<int> fully_pruned_masks;

    // Statistics
    int64_t total_cells_before   = 0;
    int64_t total_cells_after    = 0;
    int64_t total_pruned_cells   = 0;
    int64_t total_rows_skipped   = 0;   // rows skipped via Apriori
    int cuboids_materialized     = 0;
    int cuboids_fully_pruned     = 0;
    int cuboids_skipped_apriori  = 0;   // cuboids skipped because ALL parent
                                        // cuboids were themselves fully pruned

    struct CuboidSummary {
        string name;
        string dims;
        int num_dims;
        int64_t cells_before;
        int64_t cells_after;
        int64_t pruned;
        int64_t rows_skipped;
        bool materialized;
        bool skipped_entirely;
    };
    vector<CuboidSummary> summaries;

    int progress = 0;

    cout << "Computing Iceberg cuboids with Apriori pruning...\n" << endl;

    for (int level = 0; level <= K; level++) {
        cout << "--- Level " << level << " (" << levels[level].size()
             << " cuboids) ---" << endl;

        for (int mask : levels[level]) {
            auto indices    = mask_to_indices(mask, K);
            string tbl_name = make_table_name(selected_dims, indices);
            string dim_str  = mask_to_dim_string(mask, K, selected_dims);

            CuboidSummary summary;
            summary.name     = tbl_name;
            summary.dims     = dim_str;
            summary.num_dims = (int)indices.size();

            // Check if we can skip this cuboid entirely.
            bool skip_entirely = false;
            if (level >= 1) {
                auto parents = get_parent_masks(mask);
                for (int pmask : parents) {
                    if (fully_pruned_masks.count(pmask)) {
                        skip_entirely = true;
                        break;
                    }
                }
            }

            // ── Aggregation with Apriori row-skipping ──
            map<string, AggregateResult> agg_map;
            int64_t rows_skipped_this = 0;

            if (skip_entirely) {
                // Nothing to compute
            } else if (mask == 0) {
                // Apex cuboid — aggregate everything, no parents to check
                AggregateResult& agg = agg_map["ALL"];
                for (size_t row = 0; row < num_rows; row++) {
                    agg.add(measure_data[row]);
                }
            } else {
                // Get immediate parent masks for Apriori check
                auto parents = get_parent_masks(mask);

                // Filter to only parents that have pruned keys
                vector<int> active_parents;
                for (int pmask : parents) {
                    if (pruned_keys.count(pmask) && !pruned_keys[pmask].empty()) {
                        active_parents.push_back(pmask);
                    }
                }

                for (size_t row = 0; row < num_rows; row++) {
                    // Apriori check: can we skip this row?
                    bool skip_row = false;
                    for (int pmask : active_parents) {
                        string parent_key =
                            build_key_from_bits(dim_data, pmask, K, row);
                        if (pruned_keys[pmask].count(parent_key)) {
                            skip_row = true;
                            break;
                        }
                    }

                    if (skip_row) {
                        rows_skipped_this++;
                        continue;
                    }

                    // Aggregate
                    string key = build_key_from_bits(dim_data, mask, K, row);
                    agg_map[key].add(measure_data[row]);
                }
            }

            total_rows_skipped += rows_skipped_this;

            // ── Prune cells below threshold ──
            int64_t cells_before = (int64_t)agg_map.size();
            int64_t pruned_count = 0;
            unordered_set<string> this_pruned;

            for (auto it = agg_map.begin(); it != agg_map.end(); ) {
                if (it->second.sum_amount < threshold) {
                    this_pruned.insert(it->first);
                    it = agg_map.erase(it);
                    pruned_count++;
                } else {
                    ++it;
                }
            }

            // Store pruned keys for this cuboid (children will read them)
            if (!this_pruned.empty()) {
                pruned_keys[mask] = std::move(this_pruned);
            }

            int64_t cells_after = (int64_t)agg_map.size();
            total_cells_before += cells_before;
            total_cells_after  += cells_after;
            total_pruned_cells += pruned_count;

            summary.cells_before    = cells_before;
            summary.cells_after     = cells_after;
            summary.pruned          = pruned_count;
            summary.rows_skipped    = rows_skipped_this;
            summary.skipped_entirely = skip_entirely;

            if (agg_map.empty()) {
                cuboids_fully_pruned++;
                fully_pruned_masks.insert(mask);
                if (skip_entirely) cuboids_skipped_apriori++;
                summary.materialized = false;
            } else {
                write_iceberg_csv(output_dir, tbl_name, selected_dims,
                                  mask, K, agg_map);
                cuboids_materialized++;
                summary.materialized = true;
            }

            summaries.push_back(summary);
            progress++;

            if (progress % 10 == 0 || progress == total_cuboids) {
                cout << "  Processed " << progress << "/" << total_cuboids
                     << "  (materialized: " << cuboids_materialized
                     << ", pruned: " << cuboids_fully_pruned
                     << ", rows skipped by Apriori: " << total_rows_skipped
                     << ")" << endl;
            }
        }
    }

    // ── Summary report ─────────────────────────────────────────────────────
    cout << "\n========================================================="
         << "\n  ICEBERG CUBOID GENERATION — SUMMARY (Apriori-Optimized)"
         << "\n========================================================="
         << "\n  Threshold (min SUM sales)   : " << threshold
         << "\n  Total lattice nodes         : " << total_cuboids
         << "\n  Cuboids materialized        : " << cuboids_materialized
         << "\n  Cuboids fully pruned        : " << cuboids_fully_pruned
         << "\n  Cuboids skipped (Apriori)   : " << cuboids_skipped_apriori
         << "\n---------------------------------------------------------"
         << "\n  Total cells (before prune)  : " << total_cells_before
         << "\n  Total cells (after prune)   : " << total_cells_after
         << "\n  Cells pruned                : " << total_pruned_cells
         << "\n  Rows skipped (Apriori)      : " << total_rows_skipped
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
         << setw(16) << "Rows Skipped"
         << setw(14) << "Status"
         << endl;
    cout << string(122, '-') << endl;

    for (auto& s : summaries) {
        string status;
        if (s.skipped_entirely) status = "SKIP(Apriori)";
        else if (!s.materialized) status = "PRUNED";
        else status = "KEPT";

        cout << left  << setw(50) << s.name
             << right << setw(6)  << s.num_dims
             << setw(12) << s.cells_before
             << setw(12) << s.cells_after
             << setw(12) << s.pruned
             << setw(16) << s.rows_skipped
             << setw(14) << status
             << endl;
    }

    cout << "\nOutput directory: " << output_dir << "/" << endl;
    cout << "Done!" << endl;

    return 0;
}
