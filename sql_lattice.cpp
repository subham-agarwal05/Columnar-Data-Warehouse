#include<bits/stdc++.h>
#include <mysql.h>
using namespace std;

struct Config {
    string host     = "127.0.0.1";
    int port        = 3306;
    string user     = "root";
    string password = "root123";
    string database = "cuboid_warehouse";
};

class MySQLConnection {
private:
    MYSQL* conn;
public:
    MySQLConnection() : conn(nullptr) {}
    bool connect(const Config& cfg) {
        conn = mysql_init(nullptr);
        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), nullptr, cfg.port,
                                nullptr, 0)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << endl;
            return false;
        }
        return true;
    }
    bool execute(const string& query, size_t& num_rows) {
        if (mysql_query(conn, query.c_str())) {
            return false;
        }
        MYSQL_RES* res = mysql_store_result(conn);
        if (res) {
            num_rows = mysql_num_rows(res);
            mysql_free_result(res);
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

string abbreviate(const string& name) {
    static unordered_map<string, string> abbr = {
        {"category", "cat"}, {"brand", "brd"}, {"customer_segment", "cseg"},
        {"store_type", "styp"}, {"salesperson_role", "srole"}, {"campaign_name", "cname"}
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

int main(int argc, char* argv[]) {
    double threshold = -1;
    string agg_func = "";
    bool is_iceberg = false;

    if (argc >= 3) {
        threshold = stod(argv[1]);
        agg_func = argv[2];
        is_iceberg = true;
    }

    Config cfg;
    MySQLConnection db;
    if (!db.connect(cfg)) return 1;
    db.execute("USE " + cfg.database + ";", *(new size_t));

    vector<string> dims = {
        "category", "brand", "customer_segment",
        "store_type", "salesperson_role", "campaign_name"
    };

    int K = dims.size();
    int total_cuboids = 1 << K;
    
    db.execute("SET SESSION query_cache_type = OFF;", *(new size_t));
    size_t total_rows_generated = 0;

    for(int mask=0; mask<total_cuboids; mask++) {
        string group_by = "";
        string select_dims = "";
        vector<int> current_indices;
        
        for(int i=0; i<K; i++) {
            if(mask & (1<<i)) {
                if(!group_by.empty()) { group_by += ", "; select_dims += ", "; }
                group_by += dims[i];
                select_dims += dims[i];
                current_indices.push_back(i);
            }
        }
        
        string tab_name = make_table_name(dims, current_indices);
        
        string query = "SELECT ";
        if(select_dims.empty()) {
            query += "SUM(total_amount), COUNT(*), AVG(total_amount), MIN(total_amount), MAX(total_amount) FROM fact_sales_raw";
            if (is_iceberg) {
                // Formatting threshold float to skip scientific notation in query ideally, but stod handles basic cases
                char thbuf[64]; snprintf(thbuf, sizeof(thbuf), "%.2f", threshold);
                query += " HAVING " + agg_func + "(total_amount) >= " + string(thbuf);
            }
        } else {
            query += select_dims + ", SUM(total_amount), COUNT(*), AVG(total_amount), MIN(total_amount), MAX(total_amount) FROM fact_sales_raw GROUP BY " + group_by;
            if (is_iceberg) {
                char thbuf[64]; snprintf(thbuf, sizeof(thbuf), "%.2f", threshold);
                query += " HAVING " + agg_func + "(total_amount) >= " + string(thbuf);
            }
            query += ";";
        }
        
        size_t rows = 0;
        if(!db.execute(query, rows)) {
            cerr << "Failed to generate cuboid mask " << mask << endl;
        }
        if (rows > 0) { // Iceberg might filter out the table completely natively
            cout << tab_name << ":" << rows << endl;
            total_rows_generated += rows;
        }
    }
    
    cout << "TOTAL:" << total_rows_generated << endl;
    return 0;
}
