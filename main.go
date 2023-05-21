package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

const inputfile = "./data/sales.csv"
const dbHost = "localhost"
const dbPort = "15432"
const dbName = "reportdata"
const dbUser = "postgres"
const dbPassword = "password"

func main() {
	// read csv
	csvData := readCsv(inputfile)
	tableName := "product_sales"

	log.Println("header=", csvData[0])
	for i, v := range(csvData[0]) {
		log.Printf("%v=%v", i, v)
	}
	log.Println("content=", csvData[1:])

	c0, err := countRows(tableName)
	if err != nil {
		log.Fatal("check table name or db conn details")
	}
	
	dbInsertData(csvData, tableName)
	
	dbSelectData(tableName)
	c1, err := countRows(tableName)
	if err != nil {
		log.Fatal("check table name or db conn details")
	}

	log.Printf("Number of rows before inserting=%d, after inserting=%d", c0, c1)
	log.Printf("Number of rows inserted=%d", c1 - c0)
}

func readCsv(filename string)[][]string  {
	csvData := [][] string{}

	// Open the CSV file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Failed to open CSV file:", err)
	}
	defer file.Close()

	// Create a new CSV reader
	reader := csv.NewReader(file)

	// Read all records from the CSV file
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal("Failed to read CSV records:", err)
	}

	// Insert each record into the PostgreSQL table
	for _, record := range records {
		// Assuming the CSV file has three columns: col1, col2, col3
		row := make([]string, 0)
		row = append(row, record...)

		csvData = append(csvData, row)
	}

	log.Println(csvData)
	return csvData
}


func dbInsertData(csvData [][]string, tableName string)  {
	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
	dbHost, dbPort, dbName, dbUser, dbPassword))
	if err != nil {
	log.Fatal("Failed to connect to the database:", err)
	}
	defer db.Close()

	columns := strings.Join(csvData[0], ",")
	log.Println(columns)
	indexs := make([]string, 0)
	for i, _ := range(csvData[0]) {
		indexs = append(indexs, "$"+strconv.Itoa(i+1))
	}
	indexString := strings.Join(indexs, ",")
	log.Println(indexString)


	sqlStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, indexString)

	for _, row := range(csvData[1:]) {
		// Prepare the SQL statement
		log.Println("inserting row=", row)
		stmt, err := db.Prepare(sqlStmt)
		if err != nil {
			log.Fatal("Failed to prepare SQL statement:", err)
		}

		// Execute the SQL statement
		_, err = stmt.Exec(row[0],row[1],row[2],row[3],row[4], row[5])
		if err != nil {
			log.Fatal("Failed to insert record:", err)
		}
	}

}

func dbSelectData(table string)  {
	// PostgreSQL connection information
	sqlStmt := fmt.Sprintf("select * from %s LIMIT 10", table)

	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		dbHost, dbPort, dbName, dbUser, dbPassword))
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
	}
	defer db.Close()

	// Execute the SQL statement
	rows, err := db.Query(sqlStmt)
	if err != nil {
		log.Fatal("Failed to execute SELECT statement:", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Declare variables to store the column values
		var col1, col2, col3, col4, col5, col6, col7 string

		// Scan the row's columns into the variables
		err := rows.Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}

		// Process the column values as needed
		fmt.Println("Column 1:", col1)
		fmt.Println("Column 2:", col2)
		fmt.Println("Column 3:", col3)
		fmt.Println("Column 4:", col4)
		fmt.Println("Column 5:", col5)
		fmt.Println("Column 6:", col6)
		fmt.Println("Column 7:", col7)
	}

	// Check for any errors encountered during iteration
	if err := rows.Err(); err != nil {
		log.Fatal("Error occurred during iteration:", err)
	}
}

func countRows(tableName string) (int32, error) {
	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		dbHost, dbPort, dbName, dbUser, dbPassword))
	
	var count int32
	if err != nil {
		log.Println("Failed to connect to the database:", err)
		return count, err
	}
	defer db.Close()

	err = db.QueryRow(fmt.Sprintf("select count(0) from %s", tableName)).Scan(&count)
	if err != nil {
		log.Println(err)
		return count, err
	}

	return count, nil
}
