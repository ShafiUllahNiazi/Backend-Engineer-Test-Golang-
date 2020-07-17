package main

import (

	"bufio"
    "encoding/csv"
    "encoding/json"
	"fmt"
	"net"
    "io"
    "log"
	"os"
	"strconv"
	"strings"
)

var dataRow []Covid_Data  // for storing the retrieved data from csv file

const (
    CONN_HOST = "localhost"
    CONN_PORT = "4040"
    CONN_TYPE = "tcp"
)

// struct to save required columns in CSV File
type Covid_Data struct {
	Date string   `json:"date"`
    CumulativeTestPositive int   `json:"positive"`
	CumulativeTestsPerformed  int   `json:"tests"`
	Expired int   `json:"expired"`
	Admitted int   `json:"admitted"`
	Discharged int   `json:"discharged"`
	Region string   `json:"region"`

}

// struct for json query that asks for required response
type MyQuery struct {
	Query Query   `json:"query"`
}

type Query struct {
	Region string   `json:"region"`
	Date string   `json:"date"`
}


func main() {
	
	go csvFileReading()		// jumps to csvFileReading function to read CSV File
	fmt.Println("server listening on 4040")
	listener, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
        fmt.Println("Error listening:", err.Error())
	}
	defer listener.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
    for {
        // Listen for an incoming connection.
        conn, err := listener.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
		}
		fmt.Println("new connection")
        // Handle connections in a new goroutine.
		go listenConnection(conn) // jumps to the function whre requests are handled
		
    }
}

func listenConnection(conn net.Conn) {
	for{
		buffer := make([]byte, 1400)
		dataSize, err := conn.Read(buffer)		// reading the message
		if err != nil {
			fmt.Println("connection closed :", err.Error())
			return
		}
		
		data := buffer[:dataSize]
		// fmt.Println("received Message", string(data))
		str := data
		var res MyQuery
		json.Unmarshal([]byte(str), &res)	// extracting the query msg from received msg
		
		// Decesion to check based on date or region
		if (res.Query.Region != "" && res.Query.Date == ""){
			go searchByRegion(conn, res.Query.Region)
		}else if (res.Query.Region == "" && res.Query.Date != ""){
			go searchByDate(conn,res.Query.Date)
		}else {
			
			var invalidMsg = []byte("Invalid Query Try Again")

			conn.Write(invalidMsg)
		}
	}

}

// function to read csv file with required instructions
func csvFileReading()  {
	csvFile, _ := os.Open("covid_final_data.csv")
    reader := csv.NewReader(bufio.NewReader(csvFile))
	
	var i=0
	
    for {
		if i==0{
			_ , error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				log.Fatal(error)
			}
			i++
			continue
		}
		
        line, error := reader.Read()
        if error == io.EOF {
            break
        } else if error != nil {
            log.Fatal(error)
		}
	
		var ctpos int
		ctpos, _ = strconv.Atoi(line[2])
		var ctper int
		ctper, _ = strconv.Atoi(line[3])
		var dis int
		dis, _ = strconv.Atoi(line[5])
		var exp int
		exp, _ = strconv.Atoi(line[6])
		var admt int
		admt, _ = strconv.Atoi(line[10])

		dataRow = append(dataRow, Covid_Data{
		
			CumulativeTestPositive: ctpos,
			CumulativeTestsPerformed:  ctper,
			Date: line[4],
			Discharged:  dis,
			Expired: exp,
			Admitted:  admt,
			Region: line[9],			
		})
	
        
    }
    // dataRowJson, _ := json.MarshalIndent(dataRow,"","  ")
   
}

// function to fetch results based on date 
func searchByDate(conn net.Conn, queryDate1 string)  {
	
	queryDate := queryDate1
	var filteredData []Covid_Data	// for saving required data
	var itemDay int
	var itemMonth int
	var itemYear int
	for _,dataRowItem := range dataRow {

		itemDate := dataRowItem.Date
		itemDateR := strings.FieldsFunc(itemDate, func(r rune) bool { return strings.ContainsRune(" /-", r) })
		// itemDateR := strings.Split(itemDate, "/")
		queryDateR := strings.Split(queryDate, "-")
		var queryDay int
		queryDay, _ = strconv.Atoi(queryDateR[2])
		var queryMonth int
		queryMonth, _ = strconv.Atoi(queryDateR[1])
		var queryYear int
		queryYear, _ = strconv.Atoi(queryDateR[0])
		

		if(itemDateR[1] == "Mar"){
			itemDay, _ = strconv.Atoi(itemDateR[0])
			itemMonth = 3
			itemYear, _ = strconv.Atoi(itemDateR[2])

		}else{

			itemDay, _ = strconv.Atoi(itemDateR[1])
			itemMonth, _ = strconv.Atoi(itemDateR[0])
			itemYear, _ = strconv.Atoi(itemDateR[2])
		}

				

		// saving data based on date
        if (itemDay == queryDay && itemMonth == queryMonth && itemYear == queryYear) {
			filteredData = append(filteredData, dataRowItem)
        }
		
	}

	filteredDataJson, _ := json.MarshalIndent(filteredData,"","  ")
	// fmt.Println(string(filteredDataJson))
	// writing the response to client
	conn.Write(filteredDataJson)
	
	
}
func searchByRegion(conn net.Conn, region1 string)  {

	region := region1
	var filteredData []Covid_Data	// for saving required data
	for _,dataRowItem := range dataRow {

		itemRegion := dataRowItem.Region
		// saving data based on region
        if (region == itemRegion) {
			filteredData = append(filteredData, dataRowItem)		
        }
		
	}

	filteredDataJson, _ := json.MarshalIndent(filteredData,"","  ")
	// fmt.Println(string(filteredDataJson))

	// writing the response to client
	conn.Write(filteredDataJson)

	
	
}