package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

var
(t int = -1
no_of_transactions = 0
)


type KVStore struct {
	store map[string]string
}

func read(mm map[string]string, key string) {
	val, error := mm[key]
	if error {
		fmt.Println(val)
	} else {
		fmt.Println("Key not found:", key)
	}
}

func start_new_transction(s []KVStore) []KVStore {

	result := make([]KVStore, len(s))
	copy(result, s)
	return result
}

func write(s []KVStore, local map[string]string) []KVStore {

	s[t].store = make(map[string]string)

	for k, v := range local {
		s[t].store[k] = v
	}

	return s
}

func repl_handler(kv []KVStore) {

	local_store := make(map[string]string)

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">")
		text, _ := reader.ReadString('\n')
		words := strings.Fields(text)

		//Convert string to lower case
		for w := range words {
			words[w] = strings.ToLower(words[w])
		}

		switch words[0] {

		case "read":

			if len(kv) == 0 {
				t = t + 1
				l := KVStore{store: make(map[string]string)}
				(kv) = append(kv, l)
			}

			if len(words) == 2 {

				read(local_store, words[1])
			} else {
				fmt.Println("Incorrect args")
			}

		case "write":
			if t < 0 {

				t = t + 1
				l := KVStore{store: make(map[string]string)}
				(kv) = append(kv, l)

			}

			if len(words) == 3 {

				local_store[words[1]] = words[2]
				write(kv, local_store)
			} else {
				fmt.Println("Incorrect args")

			}

			fmt.Println("Rootstore", kv)

		case "delete":

			if len(words) != 2 {
				fmt.Println("Incorrect args")
				continue
			}
			_, ok := local_store[words[1]]
			if ok {
				delete(local_store, words[1])
				if t > -1 {
					delete(kv[t].store, words[1])
				}
			} else {
				fmt.Println("Key doesnt exist")
			}

		case "start":

			t = t + 1
			no_of_transactions += 1
			y := start_new_transction((kv))
			l := KVStore{store: local_store}
			kv = nil

			temp := make(map[string]string)
			for a, b := range local_store {
				temp[a] = b
			}

			local_store = map[string]string{}

			for a, b := range temp {
				local_store[a] = b
			}

			kv = make([]KVStore, len(y))
			kv = append(y, l)

			fmt.Println("Started a new transaction", kv)

		case "commit":

			if no_of_transactions > 0 {
				no_of_transactions = no_of_transactions - 1

				if t >= 1 {
					commit_to_parent := make(map[string]string)
					parent := make(map[string]string)

					//transaction to be commited
					for key, value := range kv[t].store {
						commit_to_parent[key] = value

					}

					//current parent map
					for key, value := range kv[t-1].store {

						parent[key] = value
					}

					//commit to parent map
					for key, value := range commit_to_parent {
						parent[key] = value
					}

					kv[t-1].store = make(map[string]string)
					for key, value := range commit_to_parent {
						kv[t-1].store[key] = value
					}

					//close the transaction by deleting entry in root store
					local_store = make(map[string]string)
					result := make([]KVStore, len(kv)-1)
					copy(result, kv)

					for key, value := range kv[t].store {
						local_store[key] = value
					}

					kv = make([]KVStore, len(result))
					copy(kv, result)

				}
				t = t - 1

			} else {

				fmt.Println("No transaction to commit")

			}

		case "abort":

			if no_of_transactions > 0 {
				no_of_transactions = no_of_transactions - 1
				local_store = make(map[string]string)
				result := make([]KVStore, len(kv)-1)
				copy(result, kv)

				t = t - 1
				if t >= 0 {
					for key, value := range kv[t].store {
						local_store[key] = value
					}
				}
				kv = make([]KVStore, len(result))
				copy(kv, result)
			} else {
				fmt.Println("No transaction to abort")
			}

		case "quit":

			fmt.Println("Exiting....")
			os.Exit(0)

		}
	}

}

func main() {

	k := []KVStore{}
	//kk:=&k
	repl_handler(k)

}
