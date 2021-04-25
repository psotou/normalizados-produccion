package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"

	_ "github.com/denisenkom/go-mssqldb"
)

var (
	conn    *string
	process *string
	inDir   *string
	connStr string
	// pyScript   *string
	db *sql.DB
)

func main() {
	// pyScript = flag.String("s", "./", "path to the py script. Default to current location.")
	conn = flag.String("c", "dev", "enter DB server: dev, prod, sbx")
	process = flag.String("p", "", "enter process: tables, utils, sps")
	inDir = flag.String("d", "", "enter in dir where .sql files are located (only for -p=process")
	flag.Parse()

	switch *conn {
	case "dev":
		connStr = os.Getenv("AZSQLCONNSTR_GO_DEV")
	case "prod":
		connStr = os.Getenv("AZSQLCONNSTR_GO_PROD")
	case "sbx":
		connStr = os.Getenv("AZSQLCONNSTR_GO_SBX")
	}

	switch *process {
	case "tables":
		cmd := exec.Command("python3", "merge_create_statements.py", *inDir)
		stdout, err := cmd.StdoutPipe()
		CheckErr("", err)
		stderr, err := cmd.StderrPipe()
		CheckErr("", err)
		err = cmd.Start()
		CheckErr("", err)

		go copyOutput(stdout)
		go copyOutput(stderr)

		cmd.Wait()

		file, err := ioutil.ReadFile("all.sql")
		CheckErr("", err)

		db, err = sql.Open("sqlserver", connStr)
		CheckErr("", err)

		err = db.PingContext(context.Background())
		CheckErr("", err)

		tsql := string(file)
		result, err := db.QueryContext(context.Background(), tsql)
		CheckErr("", err)
		defer result.Close()

		fmt.Println("Creación exitosa de las tablas!")

	case "utils":
		count := 0
		files, _ := ioutil.ReadDir(*inDir)
		for _, file := range files {
			actualFile := *inDir + file.Name()
			readFile, err := ioutil.ReadFile(actualFile)
			CheckErr("", err)

			db, err = sql.Open("sqlserver", connStr)
			CheckErr("", err)

			err = db.PingContext(context.Background())
			CheckErr("", err)

			tsql := string(readFile)
			result, err := db.QueryContext(context.Background(), tsql)
			CheckErr("", err)
			defer result.Close()

			count++
			fmt.Printf("Creación exitosa del archivo %s\n", file.Name())
		}
		fmt.Printf("%v archivos creados con éxito!\n", count)

	case "sps":
		count := 0
		files, _ := ioutil.ReadDir(*inDir)
		for _, file := range files {
			actualFile := *inDir + file.Name()
			readFile, err := ioutil.ReadFile(actualFile)
			CheckErr("", err)

			db, err = sql.Open("sqlserver", connStr)
			CheckErr("", err)

			err = db.PingContext(context.Background())
			CheckErr("", err)

			tsql := string(readFile)
			result, err := db.QueryContext(context.Background(), tsql)
			CheckErr("", err)
			defer result.Close()

			count++
			fmt.Printf("Creación exitosa del archivo %s\n", file.Name())
		}
		fmt.Printf("%v archivos creados con éxito!\n", count)
	}
}

func copyOutput(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
}

// CheckErr to handle error
func CheckErr(str string, err error) {
	if err != nil {
		log.Fatal(str, err.Error())
	}
}
