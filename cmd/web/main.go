package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mini-rdbms/db/engine"
	"mini-rdbms/db/schema"
	"net/http"
	"os"
)

var db *engine.Engine

// CORS middleware to allow GitHub Pages to call this API
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Allow requests from GitHub Pages
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		// Handle preflight requests
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next(w, r)
	}
}

func main() {
	db = engine.NewEngine()

	// Setup Schema and Seed Data
	setupSchema()
	seedData()

	http.HandleFunc("/users", corsMiddleware(handleUsers))
	http.HandleFunc("/orders", corsMiddleware(handleOrders))
	http.HandleFunc("/", handleHome)

	// Use PORT from environment (Railway) or default to 8080
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Printf("Server running on :%s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "cmd/web/index.html")
}

func setupSchema() {
	// Attempt Create Tables. Ignore error if exists (handled by Engine).
	db.Execute(context.Background(), "CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT UNIQUE, email TEXT)")
	db.Execute(context.Background(), "CREATE TABLE IF NOT EXISTS orders (id INT PRIMARY KEY, user_id INT, amount INT, description TEXT)")

	// Programmatically add FK constraint: orders.user_id -> users.id
	// Since we don't parse FK syntax yet, we add it directly to the table definition
	if ordersTable, ok := db.Tables["orders"]; ok {
		ordersTable.Def.ForeignKeys = []schema.ForeignKeyDef{
			{
				Column:    "user_id",
				RefTable:  "users",
				RefColumn: "id",
			},
		}
	}
}

func seedData() {
	ctx := context.Background()

	// Only seed if users table is empty
	res, _ := db.Execute(ctx, "SELECT * FROM users")
	if res != nil && len(res.Rows) > 0 {
		return
	}

	log.Println("Seeding sample data...")

	// Sample Users
	users := []string{
		"INSERT INTO users VALUES (1, 'Brian Kinyua', 'kinyua@example.com')",
		"INSERT INTO users VALUES (2, 'Jane Kamau', 'jane.k@pesapal.co.ke')",
		"INSERT INTO users VALUES (3, 'David Omari', 'omari@nairobi.go.ke')",
	}

	for _, sql := range users {
		db.Execute(ctx, sql)
	}

	// Sample Orders
	orders := []string{
		"INSERT INTO orders VALUES (5001, 1, 250, 'Large Samosa Platters')",
		"INSERT INTO orders VALUES (5002, 2, 45, 'Chapati Madondo')",
		"INSERT INTO orders VALUES (5003, 2, 120, 'Grilled Sukuma & Ugali')",
		"INSERT INTO orders VALUES (5004, 3, 3500, 'PesaPal API Credits')",
	}

	for _, sql := range orders {
		db.Execute(ctx, sql)
	}

	log.Println("Seeding complete.")
}

func handleUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		// Create User
		// JSON: { "id": 1, "name": "Alice", "email": "a@b.com" }
		var u struct {
			ID    int    `json:"id"`
			Name  string `json:"name"`
			Email string `json:"email"`
		}
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		sql := fmt.Sprintf("INSERT INTO users VALUES (%d, '%s', '%s')", u.ID, u.Name, u.Email)
		res, err := db.Execute(r.Context(), sql)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"message": res.Message})

	} else if r.Method == http.MethodGet {
		// List Users
		// Optional ?id=X
		id := r.URL.Query().Get("id")
		var sql string
		if id != "" {
			sql = fmt.Sprintf("SELECT * FROM users WHERE id = %s", id)
		} else {
			sql = "SELECT * FROM users"
		}

		res, err := db.Execute(r.Context(), sql)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// Convert structure for JSON
		resp := make([]map[string]interface{}, 0)
		for _, row := range res.Rows {
			item := make(map[string]interface{})
			for i, col := range res.Columns {
				// Simplified type handling
				v := row.Values[i]
				if v.Type == "INT" {
					val, _ := v.AsInt()
					item[col] = val
				} else {
					val, _ := v.AsText()
					item[col] = val
				}
			}
			resp = append(resp, item)
		}
		json.NewEncoder(w).Encode(resp)
	}
}

func handleOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		type OrderReq struct {
			ID          int    `json:"id"`
			UserID      int    `json:"user_id"`
			Amount      int    `json:"amount"`
			Description string `json:"description"`
		}
		var o OrderReq
		if err := json.NewDecoder(r.Body).Decode(&o); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		sql := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d, '%s')", o.ID, o.UserID, o.Amount, o.Description)
		if _, err := db.Execute(r.Context(), sql); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"result": "ok"})
	} else if r.Method == http.MethodGet {
		// Join Example: ?details=true for joining with users
		details := r.URL.Query().Get("details")
		var sql string
		if details == "true" {
			// JOIN
			sql = "SELECT orders.id, orders.amount, orders.description, users.name FROM orders JOIN users ON orders.user_id = users.id"
		} else {
			sql = "SELECT * FROM orders"
		}

		res, err := db.Execute(r.Context(), sql)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}

		// Convert structure for JSON (Duplicate logic, simplified)
		resp := make([]map[string]interface{}, 0)
		for _, row := range res.Rows {
			item := make(map[string]interface{})
			for i, col := range res.Columns {
				v := row.Values[i]
				item[col] = v.Val // interface{} is unsafe for JSON? usually ok
			}
			resp = append(resp, item)
		}
		json.NewEncoder(w).Encode(resp)
	}
}
