package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mini-rdbms/db/engine"
	"mini-rdbms/db/schema"
	"net/http"
)

var db *engine.Engine

func main() {
	db = engine.NewEngine()

	// Setup Schema if not exists
	setupSchema()

	http.HandleFunc("/users", handleUsers)
	http.HandleFunc("/orders", handleOrders)
	http.HandleFunc("/", handleHome)

	fmt.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "cmd/web/index.html")
}

func setupSchema() {
	// Attempt Create Tables. Ignore error if exists.
	db.Execute(context.Background(), "CREATE TABLE users (id INT PRIMARY KEY, name TEXT UNIQUE, email TEXT)")
	db.Execute(context.Background(), "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, description TEXT)")

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
			sql = "SELECT orders.id, orders.amount, users.name FROM orders JOIN users ON orders.user_id = users.id"
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
