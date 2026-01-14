package main

import (
	"bufio"
	"context"
	"fmt"
	"mini-rdbms/db/engine"
	"os"
	"strings"
	"text/tabwriter"
)

func main() {
	db := engine.NewEngine()

	// Ensure data directory exists if not done
	// Logic handled in Load/Save usually

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Minimal RDBMS REPL")
	fmt.Println("Type 'exit' or 'quit' to close.")

	for {
		fmt.Print("db> ")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		input = strings.TrimSpace(input)

		if input == "" {
			continue
		}
		if strings.EqualFold(input, "exit") || strings.EqualFold(input, "quit") {
			break
		}

		// Handle input ending with semicolon?
		input = strings.TrimSuffix(input, ";")

		res, err := db.Execute(context.Background(), input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		printResult(res)
	}
}

func printResult(res *engine.ResultSet) {
	if res.Message != "" {
		fmt.Println(res.Message)
		return
	}

	if len(res.Columns) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
		// Header
		for i, col := range res.Columns {
			fmt.Fprintf(w, "%s", col)
			if i < len(res.Columns)-1 {
				fmt.Fprint(w, "\t")
			}
		}
		fmt.Fprintln(w)

		// Rows
		for _, row := range res.Rows {
			for i, val := range row.Values {
				fmt.Fprintf(w, "%v", val.String())
				if i < len(row.Values)-1 {
					fmt.Fprint(w, "\t")
				}
			}
			fmt.Fprintln(w)
		}
		w.Flush()
	}
}
