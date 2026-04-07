#include<bits/stdc++.h>
#include "pugixml.hpp"
#include <mysql.h>

using namespace std;
using namespace pugi;
namespace fs = filesystem;

const string CUBOID_OFFSET_FILE = "DB/.cuboid_offset";

struct Config {
    string host     = "127.0.0.1";
    int port        = 3306;
    string user     = "root";
    string password = "root123";
    string database = "cuboid_warehouse";
};

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

struct DimColumnInfo {
    string name;
    string type;
    string attribute_type;
    string file_path;
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

// MySQL Connection wrapper
class MySQLConnection {
private:
    MYSQL* conn;
public:
    MySQLConnection() : conn(nullptr) {}

    bool connect(const Config& cfg) {
        conn = mysql_init(nullptr);
        if (!conn) { cerr << "mysql_init() failed" << endl; return false; }
        mysql_options(conn, MYSQL_OPT_LOCAL_INFILE, nullptr);
        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), nullptr, cfg.port,
                                nullptr, CLIENT_LOCAL_FILES)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << endl;
            return false;
        }
        return true;
    }

    bool execute(const string& query) {
        if (mysql_query(conn, query.c_str())) {
            cerr << "Query failed: " << mysql_error(conn) << endl;
            cerr << "Query: " << query.substr(0, 200) << endl;
            return false;
        }
        return true;
    }

    vector<vector<string>> query(const string& q) {
        vector<vector<string>> rows;
        if (mysql_query(conn, q.c_str())) {
            cerr << "Query failed: " << mysql_error(conn) << endl;
            return rows;
        }
        MYSQL_RES* result = mysql_store_result(conn);
        if (!result) return rows;
        int num_fields = mysql_num_fields(result);
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(result))) {
            vector<string> r;
            for (int i = 0; i < num_fields; i++)
                r.push_back(row[i] ? row[i] : "");
            rows.push_back(r);
        }
        mysql_free_result(result);
        return rows;
    }

    void close() { if (conn) { mysql_close(conn); conn = nullptr; } }
    ~MySQLConnection() { close(); }
};

// Schema parsers
map<string, DimColumnInfo> parse_dim_schema(const string& path) {
    map<string, DimColumnInfo> result;
    xml_document doc;
    if (!doc.load_file(path.c_str())) { cerr << "Error: Failed to parse " << path << endl; return result; }
    for (xml_node col = doc.child("DimensionalSchema").child("Column"); col; col = col.next_sibling("Column")) {
        DimColumnInfo info;
        info.name = col.attribute("name").value();
        info.type = col.attribute("type").value();
        info.attribute_type = col.attribute("attribute_type").value();
        result[info.name] = info;
    }
    return result;
}

// CDS schema entry: file path + optional dictionary path
struct CdsColumnInfo {
    string file;
    string dict;  // empty if not dictionary-encoded
};

map<string, CdsColumnInfo> parse_cds_schema(const string& path) {
    map<string, CdsColumnInfo> result;
    xml_document doc;
    if (!doc.load_file(path.c_str())) { cerr << "Error: Failed to parse " << path << endl; return result; }
    xml_node table = doc.child("ColumnStoreSchema").child("Table");
    for (xml_node col = table.child("Column"); col; col = col.next_sibling("Column")) {
        CdsColumnInfo info;
        info.file = col.attribute("file").value();
        info.dict = col.attribute("dict").value();
        result[col.attribute("name").value()] = info;
    }
    return result;
}

// Dictionary loader: reads a .dict.bin file into a vector<string>
vector<string> load_dictionary(const string& dict_path) {
    vector<string> dict;
    ifstream in(dict_path, ios::binary);
    if (!in.is_open()) { cerr << "Error: Cannot open dictionary " << dict_path << endl; return dict; }
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

// Column loaders — non-encoded strings (uint16_t length + raw bytes)
vector<string> load_column_raw(const string& file_path) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Error: Cannot open " << file_path << endl; return data; }
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

// Column loader — dictionary-encoded (uint32_t codes, decoded via dictionary)
vector<string> load_column_dict(const string& file_path, const vector<string>& dict) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Error: Cannot open " << file_path << endl; return data; }
    uint32_t code;
    while (in.read(reinterpret_cast<char*>(&code), sizeof(uint32_t))) {
        if (code < dict.size()) {
            data.push_back(dict[code]);
        } else {
            data.push_back("<INVALID>");
        }
    }
    return data;
}

// Smart column loader: auto-detects encoding from CDS schema
vector<string> load_column(const CdsColumnInfo& col_info) {
    if (!col_info.dict.empty()) {
        auto dict = load_dictionary(col_info.dict);
        return load_column_dict(col_info.file, dict);
    }
    return load_column_raw(col_info.file);
}

// Measure loader (binary format: raw float values, 4 bytes each)
vector<double> load_measure_column(const string& file_path) {
    vector<double> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Error: Cannot open " << file_path << endl; return data; }
    float val;
    while (in.read(reinterpret_cast<char*>(&val), sizeof(float))) {
        data.push_back(static_cast<double>(val));
    }
    return data;
}

// Range loaders (for incremental refresh)
// Non-encoded strings: variable-length, must iterate from beginning
vector<string> load_column_range_raw(const string& file_path, size_t start, size_t end) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Cannot open " << file_path << endl; return data; }
    size_t cur = 0;
    while (in.peek() != EOF && cur < end) {
        uint16_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(uint16_t));
        if (!in) break;
        if (cur >= start) {
            string s(len, '\0');
            in.read(s.data(), len);
            data.push_back(std::move(s));
        } else {
            in.seekg(len, ios::cur);
        }
        cur++;
    }
    return data;
}

// Dictionary-encoded range: O(1) seek since each code is 4 bytes
vector<string> load_column_range_dict(const string& file_path, const vector<string>& dict, size_t start, size_t end) {
    vector<string> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Cannot open " << file_path << endl; return data; }
    in.seekg(static_cast<streamoff>(start * sizeof(uint32_t)), ios::beg);
    for (size_t i = start; i < end; i++) {
        uint32_t code;
        if (!in.read(reinterpret_cast<char*>(&code), sizeof(uint32_t))) break;
        data.push_back(code < dict.size() ? dict[code] : "<INVALID>");
    }
    return data;
}

// Smart range loader: auto-detects encoding
vector<string> load_column_range(const CdsColumnInfo& col_info, size_t start, size_t end) {
    if (!col_info.dict.empty()) {
        auto dict = load_dictionary(col_info.dict);
        return load_column_range_dict(col_info.file, dict, start, end);
    }
    return load_column_range_raw(col_info.file, start, end);
}

// Float range: O(1) seek since each record is exactly 4 bytes
vector<double> load_measure_range(const string& file_path, size_t start, size_t end) {
    vector<double> data;
    ifstream in(file_path, ios::binary);
    if (!in.is_open()) { cerr << "Cannot open " << file_path << endl; return data; }
    in.seekg(static_cast<streamoff>(start * sizeof(float)), ios::beg);
    for (size_t i = start; i < end; i++) {
        float val;
        if (!in.read(reinterpret_cast<char*>(&val), sizeof(float))) break;
        data.push_back(static_cast<double>(val));
    }
    return data;
}

// Subset generator
vector<vector<int>> generate_all_subsets(int n) {
    vector<vector<int>> subsets;
    int total = 1 << n;
    for (int mask = 0; mask < total; mask++) {
        vector<int> subset;
        for (int i = 0; i < n; i++) {
            if (mask & (1 << i)) subset.push_back(i);
        }
        subsets.push_back(subset);
    }
    return subsets;
}

string build_key(const vector<vector<string>>& dim_data, const vector<int>& dim_indices, size_t row) {
    string key;
    for (size_t i = 0; i < dim_indices.size(); i++) {
        if (i > 0) key += "|";
        key += dim_data[dim_indices[i]][row];
    }
    return key;
}

map<string, AggregateResult> compute_cuboid(
    const vector<vector<string>>& dim_data,
    const vector<double>& measure_data,
    const vector<int>& dim_indices,
    size_t num_rows
) {
    map<string, AggregateResult> agg_map;
    for (size_t row = 0; row < num_rows; row++) {
        string key = dim_indices.empty() ? "ALL" : build_key(dim_data, dim_indices, row);
        agg_map[key].add(measure_data[row]);
    }
    return agg_map;
}

// Table name helpers
string abbreviate(const string& name) {
    static unordered_map<string, string> abbr = {
        {"category", "cat"}, {"brand", "brd"}, {"customer_segment", "cseg"},
        {"store_type", "styp"}, {"salesperson_role", "srole"}, {"campaign_name", "cname"},
        {"customer_id", "cid"}, {"product_id", "pid"}, {"store_id", "sid"},
        {"salesperson_id", "spid"}, {"campaign_id", "campid"},
        {"store_manager_id", "smid"}, {"store_manager_name", "smname"},
        {"store_manager_role", "smrole"}, {"campaign_start_date", "csdate"},
        {"campaign_end_date", "cedate"}, {"campaign_start_year", "csyr"},
        {"campaign_end_year", "ceyr"}, {"campaign_start_month", "csmon"},
        {"campaign_end_month", "cemon"}, {"campaign_start_day", "csday"},
        {"campaign_end_day", "ceday"}, {"campaign_start_weekday", "cswday"},
        {"campaign_end_weekday", "cewday"}, {"campaign_start_quarter", "csqtr"},
        {"campaign_end_quarter", "ceqtr"}, {"campaign_budget", "cbudget"},
        {"residential_location", "resloc"}, {"origin_location", "orloc"},
        {"store_location", "sloc"}, {"store_name", "sname"},
        {"salesperson_name", "spname"}, {"product_name", "pname"},
        {"first_name", "fname"}, {"last_name", "lname"}, {"email", "email"}
    };
    auto it = abbr.find(name);
    return it != abbr.end() ? it->second : name;
}

string make_table_name(const vector<string>& dim_names, const vector<int>& dim_indices) {
    if (dim_indices.empty()) return "cuboid_apex";
    string name = "cuboid";
    for (int idx : dim_indices) name += "_" + dim_names[idx];
    if (name.length() <= 64) return name;
    name = "cuboid";
    for (int idx : dim_indices) name += "_" + abbreviate(dim_names[idx]);
    if (name.length() > 64) name = name.substr(0, 64);
    return name;
}

string mysql_escape(const string& s) {
    string out;
    for (char c : s) {
        if (c == '\'') out += "\\'";
        else if (c == '\\') out += "\\\\";
        else out += c;
    }
    return out;
}

// CSV writer for full generation
void write_cuboid_csv(
    const string& output_dir,
    const string& table_name,
    const vector<string>& dim_names,
    const vector<int>& dim_indices,
    const map<string, AggregateResult>& agg_map
) {
    string filepath = output_dir + "/" + table_name + ".csv";
    ofstream out(filepath);
    if (!out.is_open()) { cerr << "Error: Cannot write to " << filepath << endl; return; }

    for (int idx : dim_indices) out << dim_names[idx] << ",";
    out << "sum_amount,count,avg_amount,min_amount,max_amount" << endl;

    out << fixed << setprecision(2);
    for (auto& [key, agg] : agg_map) {
        if (!dim_indices.empty()) {
            stringstream ss(key);
            string token;
            bool first = true;
            while (getline(ss, token, '|')) {
                if (!first) out << ",";
                if (token.find(',') != string::npos || token.find('"') != string::npos)
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

// Offset helpers
size_t read_offset(const string& path) {
    ifstream in(path);
    if (!in.is_open()) return 0;
    size_t offset = 0;
    in >> offset;
    return offset;
}

void write_offset(const string& path, size_t offset) {
    ofstream out(path, ios::trunc);
    out << offset << endl;
}


int generate_cuboids(const map<string, CdsColumnInfo>& cds_schema) {
    string output_dir = "Cuboids";
    vector<string> selected_dims = get_selected_dimensions();
    string measure_col = get_measure_column();
    int K = selected_dims.size();
    int total_cuboids = 1 << K;

    cout << "  Selected " << K << " dimensions => " << total_cuboids << " cuboids." << endl;

    // Load full columns
    cout << "Loading columns from column store..." << endl;
    vector<vector<string>> dim_data(K);
    for (int i = 0; i < K; i++) {
        dim_data[i] = load_column(cds_schema.at(selected_dims[i]));
        cout << "  Loaded " << selected_dims[i] << " (" << dim_data[i].size() << " rows)" << endl;
    }
    vector<double> measure_data = load_measure_column(cds_schema.at(measure_col).file);
    cout << "  Loaded " << measure_col << " (" << measure_data.size() << " rows)" << endl;

    size_t num_rows = measure_data.size();
    for (int i = 0; i < K; i++) {
        if (dim_data[i].size() != num_rows) {
            cerr << "Error: Column " << selected_dims[i] << " row count mismatch." << endl;
            return 1;
        }
    }

    auto all_subsets = generate_all_subsets(K);

    if (!fs::exists(output_dir)) fs::create_directory(output_dir);

    cout << "Computing aggregations and writing CSV files..." << endl;
    int progress = 0;
    for (auto& subset : all_subsets) {
        string table_name = make_table_name(selected_dims, subset);
        auto agg_map = compute_cuboid(dim_data, measure_data, subset, num_rows);
        write_cuboid_csv(output_dir, table_name, selected_dims, subset, agg_map);
        progress++;
        if (progress % 10 == 0 || progress == total_cuboids)
            cout << "  Computed " << progress << "/" << total_cuboids << " cuboids..." << endl;
    }

    // Set cuboid offset to match full data
    write_offset(CUBOID_OFFSET_FILE, num_rows);
    cout << "Cuboid offset set to: " << num_rows << endl;
    cout << "Done! Output: " << output_dir << "/ (" << total_cuboids << " cuboids)" << endl;
    return 0;
}

int refresh_cuboids(const map<string, CdsColumnInfo>& cds_schema) {
    Config cfg;
    vector<string> selected_dims = get_selected_dimensions();
    string measure_col = get_measure_column();
    int K = selected_dims.size();

    // Determine new data range
    size_t db_offset = read_offset("DB/.offset");         // total rows in column store
    size_t cuboid_offset = read_offset(CUBOID_OFFSET_FILE); // rows already in cuboids

    if (db_offset <= cuboid_offset) {
        cout << "No new data. Column store has " << db_offset
             << " rows, cuboids up to date at " << cuboid_offset << "." << endl;
        return 0;
    }

    size_t new_rows = db_offset - cuboid_offset;
    cout << "Column store: " << db_offset << " rows total." << endl;
    cout << "Cuboids last refreshed at: " << cuboid_offset << " rows." << endl;
    cout << "New rows to process: " << new_rows << endl;

    // Load only new data
    cout << "Loading new data (rows " << cuboid_offset << " to " << db_offset << ")..." << endl;
    vector<vector<string>> dim_data(K);
    for (int i = 0; i < K; i++) {
        dim_data[i] = load_column_range(cds_schema.at(selected_dims[i]), cuboid_offset, db_offset);
        cout << "  Loaded " << selected_dims[i] << " (" << dim_data[i].size() << " new values)" << endl;
    }
    vector<double> measure_data = load_measure_range(cds_schema.at(measure_col).file, cuboid_offset, db_offset);
    cout << "  Loaded " << measure_col << " (" << measure_data.size() << " new values)" << endl;

    // Connect to MySQL
    cout << "Connecting to MySQL..." << endl;
    MySQLConnection db;
    if (!db.connect(cfg)) return 1;
    db.execute("USE " + cfg.database + ";");

    auto all_subsets = generate_all_subsets(K);
    int total_cuboids = all_subsets.size();
    cout << "Refreshing " << total_cuboids << " cuboid tables..." << endl;

    int progress = 0;
    for (auto& subset : all_subsets) {
        string table_name = make_table_name(selected_dims, subset);

        // Compute delta aggregation on new data only
        map<string, AggregateResult> delta_agg;
        for (size_t row = 0; row < new_rows; row++) {
            string key = subset.empty() ? "ALL" : build_key(dim_data, subset, row);
            delta_agg[key].add(measure_data[row]);
        }

        // Upsert each group into MySQL
        for (auto& [key, agg] : delta_agg) {
            vector<string> key_parts;
            if (!subset.empty()) {
                stringstream ss(key);
                string token;
                while (getline(ss, token, '|')) key_parts.push_back(token);
            }

            // Build WHERE clause
            string where_clause;
            for (size_t i = 0; i < subset.size(); i++) {
                if (i > 0) where_clause += " AND ";
                where_clause += "`" + selected_dims[subset[i]] + "`='" + mysql_escape(key_parts[i]) + "'";
            }

            // Check if row exists in MySQL
            string check_query;
            if (subset.empty())
                check_query = "SELECT sum_amount,count,min_amount,max_amount FROM " + table_name + " LIMIT 1;";
            else
                check_query = "SELECT sum_amount,count,min_amount,max_amount FROM " + table_name + " WHERE " + where_clause + " LIMIT 1;";

            auto existing = db.query(check_query);

            if (!existing.empty()) {
                // UPDATE: merge old + delta
                double old_sum = stod(existing[0][0]);
                int64_t old_count = stoll(existing[0][1]);
                double old_min = stod(existing[0][2]);
                double old_max = stod(existing[0][3]);

                double new_sum = old_sum + agg.sum_amount;
                int64_t new_count = old_count + agg.count;
                double new_avg = new_count > 0 ? new_sum / new_count : 0.0;
                double new_min = min(old_min, agg.min_amount);
                double new_max = max(old_max, agg.max_amount);

                ostringstream uq;
                uq << fixed << setprecision(2);
                uq << "UPDATE " << table_name << " SET "
                   << "sum_amount=" << new_sum << ","
                   << "count=" << new_count << ","
                   << "avg_amount=" << new_avg << ","
                   << "min_amount=" << new_min << ","
                   << "max_amount=" << new_max;
                if (!subset.empty()) uq << " WHERE " << where_clause;
                uq << ";";
                db.execute(uq.str());
            } else {
                // INSERT: new group
                ostringstream iq;
                iq << fixed << setprecision(2);
                iq << "INSERT INTO " << table_name << " (";
                for (size_t i = 0; i < subset.size(); i++)
                    iq << "`" << selected_dims[subset[i]] << "`,";
                iq << "sum_amount,count,avg_amount,min_amount,max_amount) VALUES (";
                for (size_t i = 0; i < key_parts.size(); i++)
                    iq << "'" << mysql_escape(key_parts[i]) << "',";
                iq << agg.sum_amount << "," << agg.count << "," << agg.avg() << ","
                   << agg.min_amount << "," << agg.max_amount << ");";
                db.execute(iq.str());
            }
        }

        progress++;
        if (progress % 10 == 0 || progress == total_cuboids)
            cout << "  Refreshed " << progress << "/" << total_cuboids << " cuboids..." << endl;
    }

    // Update cuboid offset
    write_offset(CUBOID_OFFSET_FILE, db_offset);
    db.close();
    cout << "Done! Cuboids refreshed with " << new_rows << " new rows." << endl;
    return 0;
}

int main(int argc, char* argv[]) {
    string mode = "generate"; // default
    if (argc > 1) mode = argv[1];

    cout << "=== Cuboid Manager ===" << endl;

    // Parse schemas (shared)
    auto dim_schema = parse_dim_schema("dim_schema.xml");
    auto cds_schema = parse_cds_schema("cds_schema.xml");
    if (dim_schema.empty() || cds_schema.empty()) {
        cerr << "Failed to parse schemas." << endl;
        return 1;
    }

    if (mode == "generate") {
        cout << "Mode: GENERATE (full computation)" << endl;
        return generate_cuboids(cds_schema);
    } else if (mode == "refresh") {
        cout << "Mode: REFRESH (incremental update)" << endl;
        return refresh_cuboids(cds_schema);
    } else {
        cerr << "Unknown mode: " << mode << endl;
        cerr << "Usage: generate_cuboids [generate|refresh]" << endl;
        return 1;
    }
}
