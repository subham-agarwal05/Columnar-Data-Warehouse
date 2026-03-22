#include<bits/stdc++.h>
#include "pugixml.hpp"
#include <mysql.h>

using namespace std;
using namespace pugi;
namespace fs = filesystem;

struct Config {
    string host     = "127.0.0.1";
    int port        = 3306;
    string user     = "root";
    string password = "root123";
    string database = "cuboid_warehouse";
    string cuboid_dir = "Cuboids";
};

class MySQLConnection {
private:
    MYSQL* conn;
public:
    MySQLConnection() : conn(nullptr) {}

    bool connect(const Config& cfg) {
        conn = mysql_init(nullptr);
        if (!conn) {
            cerr << "mysql_init() failed" << endl;
            return false;
        }

        // Enable local infile for LOAD DATA
        mysql_options(conn, MYSQL_OPT_LOCAL_INFILE, nullptr);

        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), nullptr, cfg.port,
                                nullptr, CLIENT_LOCAL_FILES)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << endl;
            return false;
        }
        cout << "Connected to MySQL at " << cfg.host << ":" << cfg.port << endl;
        return true;
    }

    bool execute(const string& query) {
        if (mysql_query(conn, query.c_str())) {
            cerr << "Query failed: " << mysql_error(conn) << endl;
            cerr << "Query was: " << query.substr(0, 200) << "..." << endl;
            return false;
        }
        return true;
    }

    void close() {
        if (conn) {
            mysql_close(conn);
            conn = nullptr;
        }
    }

    ~MySQLConnection() { close(); }
};

// ============ CSV HEADER PARSER ============
struct CuboidFile {
    string filename;
    string table_name;
    vector<string> columns;
    vector<string> col_types; // "VARCHAR(255)", "DOUBLE", "BIGINT"
};

CuboidFile parse_cuboid_csv(const string& filepath) {
    CuboidFile cf;
    cf.filename = filepath;

    // Extract table name from filename
    string basename = fs::path(filepath).stem().string();
    cf.table_name = basename;

    // Read header line
    ifstream in(filepath);
    string header;
    if (!getline(in, header)) return cf;

    // Parse comma-separated header
    stringstream ss(header);
    string col;
    while (getline(ss, col, ',')) {
        cf.columns.push_back(col);

        // Determine SQL type based on column name
        if (col == "sum_amount" || col == "avg_amount" ||
            col == "min_amount" || col == "max_amount") {
            cf.col_types.push_back("DOUBLE");
        } else if (col == "count") {
            cf.col_types.push_back("BIGINT");
        } else {
            cf.col_types.push_back("VARCHAR(255)");
        }
    }
    return cf;
}

// ============ TABLE CREATOR ============
bool create_table(MySQLConnection& db, const CuboidFile& cf) {
    // DROP existing table
    string drop_query = "DROP TABLE IF EXISTS " + cf.table_name + ";";
    if (!db.execute(drop_query)) return false;

    // BUILD CREATE TABLE statement
    string create_query = "CREATE TABLE " + cf.table_name + " (\n";
    for (size_t i = 0; i < cf.columns.size(); i++) {
        if (i > 0) create_query += ",\n";
        create_query += "    `" + cf.columns[i] + "` " + cf.col_types[i];
    }
    create_query += "\n);";

    if (!db.execute(create_query)) return false;
    return true;
}

// ============ DATA LOADER ============
bool load_data(MySQLConnection& db, const CuboidFile& cf) {
    // Convert to absolute path with forward slashes for MySQL
    string abs_path = fs::absolute(cf.filename).string();
    replace(abs_path.begin(), abs_path.end(), '\\', '/');

    string load_query = "LOAD DATA LOCAL INFILE '" + abs_path + "' "
                         "INTO TABLE " + cf.table_name + " "
                         "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
                         "LINES TERMINATED BY '\\n' "
                         "IGNORE 1 LINES;";

    if (!db.execute(load_query)) return false;
    return true;
}

// ============ CUBOID DISCOVERY ============
vector<string> discover_cuboid_files(const string& cuboid_dir) {
    vector<string> files;
    for (auto& entry : fs::directory_iterator(cuboid_dir)) {
        if (entry.path().extension() == ".csv") {
            files.push_back(entry.path().string());
        }
    }
    sort(files.begin(), files.end());
    return files;
}

int main() {
    Config cfg;

    cout << "Cuboid MySQL Loader..." << endl;

    //Discover cuboid CSV files
    cout << "[1/4] Discovering cuboid files in " << cfg.cuboid_dir << "/ ..." << endl;
    auto csv_files = discover_cuboid_files(cfg.cuboid_dir);
    if (csv_files.empty()) {
        cerr << "No CSV files found in " << cfg.cuboid_dir << "/. Run generate_cuboids first." << endl;
        return 1;
    }
    cout << "  Found " << csv_files.size() << " cuboid CSV files." << endl;

    //Connect to MySQL
    cout << "[2/4] Connecting to MySQL..." << endl;
    MySQLConnection db;
    if (!db.connect(cfg)) return 1;

    //Create database if not exists
    db.execute("CREATE DATABASE IF NOT EXISTS " + cfg.database + ";");
    db.execute("USE " + cfg.database + ";");
    cout << "  Using database: " << cfg.database << endl;

    //Parse headers and create tables
    cout << "[3/4] Creating tables..." << endl;
    vector<CuboidFile> cuboids;
    for (auto& filepath : csv_files) {
        auto cf = parse_cuboid_csv(filepath);
        if (cf.columns.empty()) {
            cerr << "  Warning: Skipping empty file " << filepath << endl;
            continue;
        }
        if (!create_table(db, cf)) {
            cerr << "  Failed to create table for " << cf.table_name << endl;
            continue;
        }
        cuboids.push_back(cf);
    }
    cout << "  Created " << cuboids.size() << " tables." << endl;

    // Step 4: Load data into tables
    cout << "[4/4] Loading data into tables..." << endl;
    int loaded = 0;
    for (auto& cf : cuboids) {
        if (load_data(db, cf)) {
            loaded++;
            if (loaded % 10 == 0 || loaded == (int)cuboids.size()) {
                cout << "  Loaded " << loaded << "/" << cuboids.size() << " cuboids..." << endl;
            }
        } else {
            cerr << "  Failed to load data for " << cf.table_name << endl;
        }
    }

    db.close();

    cout << endl << "=== Done! ===" << endl;
    cout << "Successfully created and loaded " << loaded << " cuboid tables into MySQL." << endl;
    cout << "Connect with: mysql -h 127.0.0.1 -u root -proot123 cuboid_warehouse" << endl;

    return 0;
}
