/*
 * query_cuboids.cpp
 * ─────────────────────────────────────────────────────────────────────────────
 * Menu-driven analytical query tool for the Cuboid Lattice stored in MySQL.
 *
 * The lattice was built from fact_sales_raw with 6 dimensions:
 *   category, brand, customer_segment, store_type, salesperson_role, campaign_name
 * Measure: total_amount  —  Aggregates: SUM, COUNT, AVG, MIN, MAX
 *
 * Cuboid tables follow naming convention:
 *   cuboid_<dim1>[_<dim2>...]   (e.g. cuboid_category_brand)
 *   cuboid_apex                 (0-D cuboid, grand totals)
 *
 * Compile:
 *   g++ -std=c++17 -O3 query_cuboids.cpp -o query_cuboids \
 *       -I/usr/include/mysql -L/usr/lib/x86_64-linux-gnu -lmysqlclient
 *
 * Usage:
 *   ./query_cuboids
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include <bits/stdc++.h>
#include <filesystem>
#include <mysql.h>

using namespace std;

// ═══════════════════════════════════════════════════════════════════════════
//  MySQL Helpers
// ═══════════════════════════════════════════════════════════════════════════

struct Config {
    string host     = "127.0.0.1";
    int    port     = 3306;
    string user     = "root";
    string password = "root123";
    string database = "cuboid_warehouse";
};

class MySQLConnection {
    MYSQL* conn;
    string database;
public:
    MySQLConnection() : conn(nullptr) {}

    bool connect(const Config& cfg) {
        conn = mysql_init(nullptr);
        if (!conn) { cerr << "mysql_init failed\n"; return false; }
        if (!mysql_real_connect(conn, cfg.host.c_str(), cfg.user.c_str(),
                                cfg.password.c_str(), cfg.database.c_str(),
                                cfg.port, nullptr, 0)) {
            cerr << "MySQL connection failed: " << mysql_error(conn) << "\n";
            return false;
        }
        database = cfg.database;
        return true;
    }

    bool table_exists(const string& table_name) {
        string sql = "SELECT 1 FROM information_schema.tables "
                     "WHERE table_schema = '" + database + "' "
                     "AND table_name = '" + table_name + "' LIMIT 1";
        if (mysql_query(conn, sql.c_str())) return false;
        MYSQL_RES* res = mysql_store_result(conn);
        if (!res) return false;
        bool exists = mysql_num_rows(res) > 0;
        mysql_free_result(res);
        return exists;
    }

    bool ensure_tables(const vector<string>& tables) {
        vector<string> missing;
        for (const auto& t : tables) {
            if (!table_exists(t)) missing.push_back(t);
        }
        if (missing.empty()) return true;
        cout << "\033[31mMissing tables:\033[0m ";
        for (size_t i = 0; i < missing.size(); i++) {
            if (i) cout << ", ";
            cout << missing[i];
        }
        cout << "\n";
        cout << "Load cuboids into MySQL (for example, run load_to_mysql) and try again.\n";
        return false;
    }

    static string csv_escape(const string& value) {
        bool needs_quotes = value.find_first_of(",\n\r\"") != string::npos;
        if (!needs_quotes) return value;
        string out = "\"";
        for (char c : value) {
            if (c == '\"') out += "\"\"";
            else out += c;
        }
        out += "\"";
        return out;
    }

    static void ensure_parent_dir(const string& path) {
        filesystem::path p(path);
        if (p.has_parent_path()) {
            filesystem::create_directories(p.parent_path());
        }
    }

    // Execute a query, print the result table, and optionally write CSV.
    // Returns row count.
    int run_and_print(const string& sql, const string& csv_path = "") {
        if (mysql_query(conn, sql.c_str())) {
            cerr << "\033[31mQuery error:\033[0m " << mysql_error(conn) << "\n";
            return -1;
        }
        MYSQL_RES* res = mysql_store_result(conn);
        if (!res) { cout << "(no result set)\n"; return 0; }

        int num_fields = mysql_num_fields(res);
        MYSQL_FIELD* fields = mysql_fetch_fields(res);

        // Collect all rows to compute column widths
        vector<vector<string>> rows;
        vector<int> widths(num_fields, 0);
        for (int i = 0; i < num_fields; i++)
            widths[i] = max(widths[i], (int)strlen(fields[i].name));

        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res))) {
            vector<string> r;
            for (int i = 0; i < num_fields; i++) {
                string val = row[i] ? row[i] : "NULL";
                widths[i] = max(widths[i], (int)val.size());
                r.push_back(val);
            }
            rows.push_back(r);
        }

        if (!csv_path.empty()) {
            ensure_parent_dir(csv_path);
            ofstream out(csv_path);
            if (!out) {
                cerr << "\033[31mCSV write error:\033[0m Unable to open " << csv_path << "\n";
            } else {
                for (int i = 0; i < num_fields; i++) {
                    if (i) out << ",";
                    out << csv_escape(fields[i].name);
                }
                out << "\n";
                for (const auto& r : rows) {
                    for (int i = 0; i < num_fields; i++) {
                        if (i) out << ",";
                        out << csv_escape(r[i]);
                    }
                    out << "\n";
                }
            }
        }

        // Print separator
        auto sep = [&]() {
            cout << "+";
            for (int i = 0; i < num_fields; i++)
                cout << string(widths[i] + 2, '-') << "+";
            cout << "\n";
        };

        // Header
        sep();
        cout << "|";
        for (int i = 0; i < num_fields; i++)
            cout << " \033[1m" << left << setw(widths[i]) << fields[i].name << "\033[0m |";
        cout << "\n";
        sep();

        // Rows
        for (auto& r : rows) {
            cout << "|";
            for (int i = 0; i < num_fields; i++)
                cout << " " << left << setw(widths[i]) << r[i] << " |";
            cout << "\n";
        }
        sep();

        int count = (int)rows.size();
        cout << "(" << count << " row" << (count != 1 ? "s" : "") << ")\n";
        mysql_free_result(res);
        return count;
    }

    void close() {
        if (conn) { mysql_close(conn); conn = nullptr; }
    }
    ~MySQLConnection() { close(); }
};

// ═══════════════════════════════════════════════════════════════════════════
//  Dimension metadata
// ═══════════════════════════════════════════════════════════════════════════

const vector<string> ALL_DIMS = {
    "category", "brand", "customer_segment",
    "store_type", "salesperson_role", "campaign_name"
};

string abbreviate(const string& name) {
    static unordered_map<string, string> abbr = {
        {"category", "cat"}, {"brand", "brd"}, {"customer_segment", "cseg"},
        {"store_type", "styp"}, {"salesperson_role", "srole"}, {"campaign_name", "cname"}
    };
    auto it = abbr.find(name);
    return it != abbr.end() ? it->second : name;
}

string make_table_name(const vector<int>& dim_indices) {
    if (dim_indices.empty()) return "cuboid_apex";
    string name = "cuboid";
    for (int idx : dim_indices) name += "_" + ALL_DIMS[idx];
    if (name.length() <= 64) return name;
    name = "cuboid";
    for (int idx : dim_indices) name += "_" + abbreviate(ALL_DIMS[idx]);
    if (name.length() > 64) name = name.substr(0, 64);
    return name;
}

// ═══════════════════════════════════════════════════════════════════════════
// 15 Business-Focused Queries (Cuboid Lattice)
// ═══════════════════════════════════════════════════════════════════════════

// ── Q1  (Easy) ──────────────────────────────────────────────────────────────
// Grand totals from the apex cuboid
void q1_grand_totals(MySQLConnection& db) {
    cout << "\n\033[33m══ Q1: Executive Summary ══\033[0m\n";
    cout << "Show overall sales, number of transactions, and typical order value.\n";
    if (!db.ensure_tables({"cuboid_apex"})) return;
    db.run_and_print(
        "SELECT sum_amount AS total_sales, count AS total_transactions, "
        "avg_amount AS avg_sale, min_amount AS min_sale, max_amount AS max_sale "
        "FROM cuboid_apex",
        "reports/q1_executive_summary.csv");
}

// ── Q2  (Easy) ──────────────────────────────────────────────────────────────
// Revenue share by category
void q2_sales_by_category(MySQLConnection& db) {
    cout << "\n\033[33m══ Q2: Category Revenue Share ══\033[0m\n";
    cout << "Which product categories contribute most to total revenue?\n";
    if (!db.ensure_tables({"cuboid_category", "cuboid_apex"})) return;
    db.run_and_print(
        "SELECT c.category, c.sum_amount AS category_sales, c.count AS transactions, "
        "c.avg_amount AS avg_sale, "
        "ROUND(c.sum_amount * 100.0 / a.sum_amount, 2) AS pct_of_total "
        "FROM cuboid_category c "
        "CROSS JOIN cuboid_apex a "
        "ORDER BY c.sum_amount DESC",
        "reports/q2_category_share.csv");
}

// ── Q3  (Easy) ──────────────────────────────────────────────────────────────
// Top brands by revenue and AOV
void q3_top_brands_by_avg(MySQLConnection& db) {
    cout << "\n\033[33m══ Q3: Top Brands by Revenue ══\033[0m\n";
    cout << "Which brands bring in the most revenue, and what is their average order value?\n";
    if (!db.ensure_tables({"cuboid_brand", "cuboid_apex"})) return;
    db.run_and_print(
        "SELECT b.brand, b.sum_amount AS brand_sales, b.count AS transactions, "
        "b.avg_amount AS avg_sale, "
        "ROUND(b.sum_amount * 100.0 / a.sum_amount, 2) AS pct_of_total "
        "FROM cuboid_brand b "
        "CROSS JOIN cuboid_apex a "
        "ORDER BY b.sum_amount DESC "
        "LIMIT 10",
        "reports/q3_top_brands.csv");
}

// ── Q4  (Medium) ────────────────────────────────────────────────────────────
// Category × Brand  →  Top combinations by revenue
void q4_category_brand_top5(MySQLConnection& db) {
    cout << "\n\033[33m══ Q4: Top Category and Brand Pairings ══\033[0m\n";
    cout << "Which category and brand combinations generate the highest revenue?\n";
    if (!db.ensure_tables({"cuboid_category_brand"})) return;
    db.run_and_print(
        "SELECT category, brand, sum_amount, count, avg_amount "
        "FROM cuboid_category_brand "
        "ORDER BY sum_amount DESC "
        "LIMIT 10",
        "reports/q4_category_brand_top.csv");
}

// ── Q5  (Medium) ────────────────────────────────────────────────────────────
// Customer segment performance across store types
void q5_segment_storetype(MySQLConnection& db) {
    cout << "\n\033[33m══ Q5: Segment Performance by Store Type ══\033[0m\n";
    cout << "How do customer segments perform across different store types?\n";
    if (!db.ensure_tables({"cuboid_customer_segment_store_type"})) return;
    db.run_and_print(
        "SELECT customer_segment, store_type, "
        "sum_amount, count, "
        "ROUND(sum_amount / count, 2) AS computed_avg "
        "FROM cuboid_customer_segment_store_type "
        "ORDER BY customer_segment, sum_amount DESC",
        "reports/q5_segment_storetype.csv");
}

// ── Q6  (Medium) ────────────────────────────────────────────────────────────
// Campaign performance by revenue and AOV
void q6_iceberg_filter(MySQLConnection& db) {
    cout << "\n\033[33m══ Q6: Campaign Performance ══\033[0m\n";
    cout << "Which campaigns generate the most revenue, and what is their average order value?\n";
    if (!db.ensure_tables({"cuboid_campaign_name"})) return;
    db.run_and_print(
        "SELECT campaign_name, sum_amount, count, avg_amount "
        "FROM cuboid_campaign_name "
        "ORDER BY sum_amount DESC "
        "LIMIT 15",
        "reports/q6_campaign_performance.csv");
}

// ── Q7  (Hard) ───────────────────────────────────────────────────────────────
// Salesperson role × campaign performance
void q7_drilldown_contribution(MySQLConnection& db) {
    cout << "\n\033[33m══ Q7: Salesperson Role and Campaign Effectiveness ══\033[0m\n";
    cout << "Which sales roles perform best within each campaign?\n";
    if (!db.ensure_tables({"cuboid_salesperson_role_campaign_name"})) return;
    db.run_and_print(
        "SELECT salesperson_role, campaign_name, sum_amount, count, avg_amount "
        "FROM cuboid_salesperson_role_campaign_name "
        "ORDER BY sum_amount DESC "
        "LIMIT 20",
        "reports/q7_role_campaign.csv");
}

// ── Q8  (Hard) ───────────────────────────────────────────────────────────────
// Best campaigns per customer segment (ranked)
void q8_rollup_verification(MySQLConnection& db) {
    cout << "\n\033[33m══ Q8: Best Campaigns for Each Customer Segment ══\033[0m\n";
    cout << "Which campaigns work best for each customer segment?\n";
    if (!db.ensure_tables({"cuboid_customer_segment_campaign_name"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT customer_segment, campaign_name, sum_amount, count, "
        "    RANK() OVER (PARTITION BY customer_segment ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_customer_segment_campaign_name"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY customer_segment, rnk",
        "reports/q8_segment_campaign_rank.csv");
}

// ── Q9  (Hard) ───────────────────────────────────────────────────────────────
// Top brands per store type
void q9_ranked_brands_per_category(MySQLConnection& db) {
    cout << "\n\033[33m══ Q9: Top Brands by Store Type ══\033[0m\n";
    cout << "Which brands sell best in each store type?\n";
    if (!db.ensure_tables({"cuboid_brand_store_type"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT store_type, brand, sum_amount, count, "
        "    RANK() OVER (PARTITION BY store_type ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_brand_store_type"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY store_type, rnk",
        "reports/q9_storetype_brand_rank.csv");
}

// ── Q10 (Very Hard) ─────────────────────────────────────────────────────────-
// High-value multi-dimensional segments (iceberg-style)
void q10_high_value_segments(MySQLConnection& db) {
    cout << "\n\033[33m══ Q10: High-Value Pockets to Target ══\033[0m\n";
    cout << "Which category, segment, and store-type combinations generate outsized revenue?\n";
    if (!db.ensure_tables({"cuboid_category_customer_segment_store_type"})) return;
    db.run_and_print(
        "SELECT category, customer_segment, store_type, sum_amount, count, avg_amount "
        "FROM cuboid_category_customer_segment_store_type "
        "ORDER BY sum_amount DESC "
        "LIMIT 20",
        "reports/q10_high_value_segments.csv");
}

// ── Q11 ───────────────────────────────────────────────────────────────────
// Customer segment revenue share
void q11_segment_overall(MySQLConnection& db) {
    cout << "\n\033[33m══ Q11: Customer Segment Revenue Share ══\033[0m\n";
    cout << "Which customer segments drive the most revenue overall?\n";
    if (!db.ensure_tables({"cuboid_customer_segment", "cuboid_apex"})) return;
    db.run_and_print(
        "SELECT cs.customer_segment, cs.sum_amount, cs.count, cs.avg_amount, "
        "ROUND(cs.sum_amount * 100.0 / a.sum_amount, 2) AS pct_of_total "
        "FROM cuboid_customer_segment cs "
        "CROSS JOIN cuboid_apex a "
        "ORDER BY cs.sum_amount DESC "
        "LIMIT 50",
        "reports/q11_segment_share.csv");
}

// ── Q12 ───────────────────────────────────────────────────────────────────
// Best customer segments per campaign
void q12_best_segments_per_campaign(MySQLConnection& db) {
    cout << "\n\033[33m══ Q12: Best Customer Segments per Campaign ══\033[0m\n";
    cout << "Which customer segments respond best within each campaign?\n";
    if (!db.ensure_tables({"cuboid_customer_segment_campaign_name"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT campaign_name, customer_segment, sum_amount, count, "
        "    RANK() OVER (PARTITION BY campaign_name ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_customer_segment_campaign_name"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY campaign_name, rnk "
        "LIMIT 50",
        "reports/q12_campaign_segment_rank.csv");
}

// ── Q13 ───────────────────────────────────────────────────────────────────
// Best brands per customer segment
void q13_best_brands_per_segment(MySQLConnection& db) {
    cout << "\n\033[33m══ Q13: Best Brands for Each Customer Segment ══\033[0m\n";
    cout << "Which brands sell best within each customer segment?\n";
    if (!db.ensure_tables({"cuboid_brand_customer_segment"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT customer_segment, brand, sum_amount, count, "
        "    RANK() OVER (PARTITION BY customer_segment ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_brand_customer_segment"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY customer_segment, rnk "
        "LIMIT 50",
        "reports/q13_segment_brand_rank.csv");
}

// ── Q14 ───────────────────────────────────────────────────────────────────
// Best categories per customer segment
void q14_best_categories_per_segment(MySQLConnection& db) {
    cout << "\n\033[33m══ Q14: Best Categories for Each Customer Segment ══\033[0m\n";
    cout << "Which categories perform best within each customer segment?\n";
    if (!db.ensure_tables({"cuboid_category_customer_segment"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT customer_segment, category, sum_amount, count, "
        "    RANK() OVER (PARTITION BY customer_segment ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_category_customer_segment"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY customer_segment, rnk "
        "LIMIT 50",
        "reports/q14_segment_category_rank.csv");
}

// ── Q15 ───────────────────────────────────────────────────────────────────
// Best salesperson roles per customer segment
void q15_best_roles_per_segment(MySQLConnection& db) {
    cout << "\n\033[33m══ Q15: Best Sales Roles for Each Customer Segment ══\033[0m\n";
    cout << "Which sales roles perform best within each customer segment?\n";
    if (!db.ensure_tables({"cuboid_customer_segment_salesperson_role"})) return;
    db.run_and_print(
        "SELECT * FROM ("
        "  SELECT customer_segment, salesperson_role, sum_amount, count, "
        "    RANK() OVER (PARTITION BY customer_segment ORDER BY sum_amount DESC) AS rnk "
        "  FROM cuboid_customer_segment_salesperson_role"
        ") ranked "
        "WHERE rnk <= 3 "
        "ORDER BY customer_segment, rnk "
        "LIMIT 50",
        "reports/q15_segment_role_rank.csv");
}

// ═══════════════════════════════════════════════════════════════════════════
//  Menu System
// ═══════════════════════════════════════════════════════════════════════════

void print_menu() {
    cout << "\n"
         << "\033[1;35m╔═══════════════════════════════════════════════════════════════╗\033[0m\n"
         << "\033[1;35m║                        QUERY MENU                             ║\033[0m\n"
         << "\033[1;35m╠═══════════════════════════════════════════════════════════════╣\033[0m\n"
         << "\033[1;35m║\033[0m    1.  Show overall sales, transactions, and order value      \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    2.  Which categories contribute most to revenue?           \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    3.  Which brands bring the most revenue?                   \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    4.  Which category and brand pairs lead revenue?           \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    5.  How do segments perform by store type?                 \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    6.  Which campaigns drive the most revenue?                \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    7.  Which sales roles work best in campaigns?              \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    8.  Which campaigns work best for each segment?            \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    9.  Which brands sell best in each store type?             \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   10.  Which category, segment, and store type win?           \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   11.  Which customer segments drive the most revenue?        \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   12.  Which segments respond best in each campaign?          \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   13.  Which brands sell best within each segment?            \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   14.  Which categories perform best within each segment?     \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m   15.  Which sales roles perform best within each segment?    \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m                                                               \033[1;35m║\033[0m\n"
         << "\033[1;35m║\033[0m    0.  Exit                                                   \033[1;35m║\033[0m\n"
         << "\033[1;35m╚═══════════════════════════════════════════════════════════════╝\033[0m\n"
         << "\n  Enter choice [0-15]: ";
}

int main() {
    Config cfg;
    MySQLConnection db;

    cout << "\033[1mConnecting to MySQL (" << cfg.host << ":" << cfg.port
         << ", db=" << cfg.database << ") ...\033[0m\n";
    if (!db.connect(cfg)) {
        cerr << "Failed to connect. Ensure MySQL is running and cuboid tables are loaded.\n";
        return 1;
    }
    cout << "\033[32mConnected successfully!\033[0m\n";

    int choice = -1;
    while (true) {
        print_menu();
        if (!(cin >> choice)) {
            cin.clear();
            cin.ignore(numeric_limits<streamsize>::max(), '\n');
            cout << "\033[31mInvalid input. Please enter a number 0-15.\033[0m\n";
            continue;
        }

        switch (choice) {
            case 0:
                cout << "\n\033[32mGoodbye!\033[0m\n";
                db.close();
                return 0;
            case 1:  q1_grand_totals(db);              break;
            case 2:  q2_sales_by_category(db);          break;
            case 3:  q3_top_brands_by_avg(db);          break;
            case 4:  q4_category_brand_top5(db);        break;
            case 5:  q5_segment_storetype(db);          break;
            case 6:  q6_iceberg_filter(db);             break;
            case 7:  q7_drilldown_contribution(db);     break;
            case 8:  q8_rollup_verification(db);        break;
            case 9:  q9_ranked_brands_per_category(db); break;
            case 10: q10_high_value_segments(db);       break;
            case 11: q11_segment_overall(db);           break;
            case 12: q12_best_segments_per_campaign(db);break;
            case 13: q13_best_brands_per_segment(db);   break;
            case 14: q14_best_categories_per_segment(db);break;
            case 15: q15_best_roles_per_segment(db);    break;
            default:
                cout << "\033[31mInvalid choice. Please enter 0-15.\033[0m\n";
        }

        cout << "\n\033[90mPress Enter to continue...\033[0m";
        cin.ignore(numeric_limits<streamsize>::max(), '\n');
        cin.get();
    }
}
