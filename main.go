package main

import (
	"bufio"
	"encoding/json"
	"github.com/lechefran/webtest"
	"log"
	"net/url"
	"os"
	"sync"
)

func main() {
	PingUrl := os.Getenv("PING_URL")
	InstallUrl := os.Getenv("INSTALL_URL")
	ScanGetRestaurantsUrl := os.Getenv("SCAN_GET_RESTAURANTS_URL")
	AtlasSearchGetRestaurantsUrl := os.Getenv("ATLAS_SEARCH_GET_RESTAURANTS_URL")

	var wg sync.WaitGroup

	// headers
	headers := map[string]string{}
	headers["Content-Type"] = "application/json"

	// options
	opts := webtest.WebClientOptions{
		WriteToFile: false,
	}

	// initialize client and call healthcheck endpoint
	client := webtest.InitWebClient().Headers(&headers).Options(&opts)

	if res, err := client.Get(PingUrl); err != nil {
		log.Println("Cannot connect to benchmarking service...")
		log.Println(err)
	} else {
		log.Println("Successfully connected to benchmarking service...")
		_ = res.Body.Close()
	}

	log.Println("Starting benchmark...")
	body := struct {
		Install       string `json:"install"`
		LoadIndexes   bool   `json:"loadIndexes"`
		DocumentCount int    `json:"documentCount"`
	}{
		Install:       "full",
		LoadIndexes:   true,
		DocumentCount: 2000000,
	}

	_ = bufio.NewReader(os.Stdin)
	log.Println("Press any key to continue...")
	log.Println("Starting data installation...")

	// run full installation for documents
	if bytes, err := json.Marshal(body); err != nil {
		log.Fatal(err)
	} else {
		if res, err := client.Post(InstallUrl, bytes); err != nil {
			log.Fatal(err)
		} else {
			_ = res.Body.Close()
		}
	}

	ids := webtest.ReadCsv("./csv/demo.search.ids.csv")[:10000]
	names := webtest.ReadCsv("./csv/demo.search.names.csv")[:10000]
	cities := webtest.ReadCsv("./csv/demo.search.cities.csv")[:10000]
	states := webtest.ReadCsv("./csv/demo.search.states.csv")
	countries := webtest.ReadCsv("./csv/demo.search.countries.csv")

	//log.Println("Starting column scan benchmark...")

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/col-scan-restaurant-id-results.txt",
		})
		for _, s := range ids {
			params := make(map[string]string)
			params["id"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/col-scan-owner-name-results.txt",
		})
		for _, s := range names {
			params := make(map[string]string)
			params["firstName"] = s[0]
			params["lastName"] = s[1]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/col-scan-city-results.txt",
		})
		for _, s := range cities {
			params := make(map[string]string)
			params["city"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/col-scan-state-results.txt",
		})
		for _, s := range states {
			params := make(map[string]string)
			params["state"] = s[0]
			if res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params)); err != nil {
				log.Fatal(err)
			} else {
				_ = res.Body.Close()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/col-scan-country-results.txt",
		})
		for _, s := range countries {
			params := make(map[string]string)
			params["country"] = s[0]
			if res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params)); err != nil {
				log.Fatal(err)
			} else {
				_ = res.Body.Close()
			}
		}
	}()
	wg.Wait()

	// run full installation for indexes
	_ = bufio.NewReader(os.Stdin)
	log.Println("Press any key to continue...")
	log.Println("Starting data installation...")

	body.LoadIndexes = true
	if bytes, err := json.Marshal(body); err != nil {
		log.Fatal(err)
	} else {
		res, err := client.Post(InstallUrl, bytes)
		if err != nil {
			log.Fatal(err)
		}
		err = res.Body.Close()
		if err != nil {
			log.Println(err)
		}
	}

	log.Println("Starting index scan benchmark...")

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/idx-scan-restaurant-id-results.txt",
		})
		for _, s := range ids {
			params := make(map[string]string)
			params["id"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/idx-scan-owner-name-results.txt",
		})
		for _, s := range names {
			params := make(map[string]string)
			params["firstName"] = s[0]
			params["lastName"] = s[1]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/idx-scan-city-results.txt",
		})
		for _, s := range cities {
			params := make(map[string]string)
			params["city"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/idx-scan-state-results.txt",
		})
		for _, s := range states {
			params := make(map[string]string)
			params["state"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/idx-scan-country-results.txt",
		})
		for _, s := range countries {
			params := make(map[string]string)
			params["country"] = s[0]
			res, err := client.Get(addUrlQueryParams(ScanGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	log.Println("Starting atlas search benchmark...")

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/atlas-search-restaurant-id-results.txt",
		})
		for _, s := range ids {
			params := make(map[string]string)
			params["id"] = s[0]
			params["searchIndex"] = "restaurant-id-search"
			res, err := client.Get(addUrlQueryParams(AtlasSearchGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/atlas-search-owner-name-results.txt",
		})
		for _, s := range names {
			params := make(map[string]string)
			params["firstName"] = s[0]
			params["lastName"] = s[1]
			params["searchIndex"] = "owner-name-search"
			res, err := client.Get(addUrlQueryParams(AtlasSearchGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() { // run 10k requests
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/atlas-search-city-results.txt",
		})
		for _, s := range cities {
			params := make(map[string]string)
			params["city"] = s[0]
			params["searchIndex"] = "address-search"
			res, err := client.Get(addUrlQueryParams(AtlasSearchGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/atlas-search-state-results.txt",
		})
		for _, s := range states {
			params := make(map[string]string)
			params["state"] = s[0]
			params["searchIndex"] = "address-search"
			res, err := client.Get(addUrlQueryParams(AtlasSearchGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := webtest.InitWebClient().Headers(&headers).Options(&webtest.WebClientOptions{
			WriteToFile: true,
			FilePath:    "./log/atlas-search-country-results.txt",
		})
		for _, s := range countries {
			params := make(map[string]string)
			params["country"] = s[0]
			params["searchIndex"] = "address-search"
			res, err := client.Get(addUrlQueryParams(AtlasSearchGetRestaurantsUrl, params))
			if err != nil {
				log.Fatal(err)
			}
			err = res.Body.Close()
			if err != nil {
				log.Println(err)
			}
		}
	}()
	wg.Wait()
	log.Println("Benchmark finished")
}

func addUrlQueryParams(s string, m map[string]string) string {
	parsed, err := url.Parse(s)
	if err != nil {
		log.Fatal(err)
	}

	q := parsed.Query()
	for k, v := range m {
		q.Add(k, v)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String()
}
