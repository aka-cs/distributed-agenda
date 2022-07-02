package chord

import (
	log "github.com/sirupsen/logrus"
	"math/big"
	"server/chord/chord"
	"strconv"
	"time"
)

func PrintStorage(node *Node) {
	response, err := node.Partition(nil, emptyRequest)
	if err != nil {
		log.Fatal(err.Error() + "Error obtaining storage dictionary.\n")
	}
	in := response.In
	message := "Storage: \n"
	for key, bytes := range in {
		value := string(bytes)
		message += key + ": " + value + "\n"

		keyHash, err := HashKey(key, node.config.Hash)
		if err != nil {
			log.Error("Error hashing.\n")
			return
		}
		keyID := big.Int{}
		keyID.SetBytes(keyHash)
		message += keyID.String() + "\n"
	}

	out := response.Out
	message += "Replication: \n"
	for key, bytes := range out {
		value := string(bytes)
		message += key + ": " + value + "\n"

		keyHash, err := HashKey(key, node.config.Hash)
		if err != nil {
			log.Error("Error hashing.\n")
			return
		}
		keyID := big.Int{}
		keyID.SetBytes(keyHash)
		message += keyID.String() + "\n"
	}

	log.Info(message)
}

func PrintSuccessors(node *Node) {
	message := "Successors: \n"
	node.sucLock.RLock()
	qn := node.successors.first
	node.sucLock.RUnlock()

	for qn != nil {
		node.sucLock.RLock()
		suc := qn.value
		node.sucLock.RUnlock()

		message += suc.IP + "\n"

		node.sucLock.RLock()
		qn = qn.next
		node.sucLock.RUnlock()
	}

	log.Info(message)
}

func PrintPredecessor(node *Node) {
	node.predLock.RLock()
	pred := node.predecessor
	node.predLock.RUnlock()

	if pred != nil {
		log.Info("Predecessor: \n" + pred.IP + "\n")
	} else {
		log.Info("Predecessor: \nNo predecessor.\n")
	}

	ID := big.Int{}
	ID.SetBytes(node.ID)

	predID := big.Int{}
	predID.SetBytes(pred.ID)

	log.Info("\n" + ID.String() + "\n" + predID.String() + "\n")
}

func PrintFingers(node *Node) {
	message := "Finger Table: \n"
	fingers := make(map[string]struct{})

	for i := len(node.fingerTable) - 1; i >= 0; i-- {
		node.fingerLock.RLock()
		finger := node.fingerTable[i]
		node.fingerLock.RUnlock()

		if finger == nil {
			continue
		}

		if _, ok := fingers[finger.IP]; !ok {
			message += strconv.Itoa(i) + ": " + finger.IP + "\n"
			fingers[finger.IP] = struct{}{}
		}
	}

	log.Info(message)
}

func DummySetter(node *Node, keys int) {
	ticker := time.NewTicker(1 * time.Second) // Set the time between routine activations.
	for i := 0; i < keys; {
		select {
		case <-ticker.C:
			PrintFingers(node)
			i++
			key := strconv.Itoa(i) + "-" + node.IP
			rq := chord.SetRequest{Key: key, Value: []byte(strconv.Itoa(i))}
			if _, err := node.Set(nil, &rq); err != nil {
				log.Fatal("Error setting <key, value> pair.\n" + err.Error() + "\n")
				break
			}
		}
	}
}

func Wait(duration time.Duration) {
	ticker := time.NewTicker(duration)
	for {
		select {
		case <-ticker.C:
			return
		}
	}
}

/*
func IP() net.IP {
	host, _ := os.Hostname()
	addr, _ := net.LookupIP(host)
	for _, addr := range addr {
		if ipv4 := addr.To4(); ipv4 != nil {
			return ipv4
		}
	}
	return nil
}
*/

func Test() {
	log.Info("Test version: " + strconv.Itoa(50) + "\n")
	conf := DefaultConfig()
	transport := NewGRPCServices(conf)
	dictionary := NewDictionary(conf.Hash)

	node, err := NewNode("3333", conf, transport, dictionary)
	if err != nil {
		log.Fatal("Error creating node")
	}

	if err = node.Start(); err != nil {
		log.Fatal(err.Error() + "Error starting server.\n")
		return
	}

	Wait(time.Second)
	DummySetter(node, 1)

	Wait(10 * time.Second)
	PrintStorage(node)
	PrintPredecessor(node)
	PrintSuccessors(node)
	PrintFingers(node)

	Wait(time.Minute)
	PrintStorage(node)
	PrintPredecessor(node)
	PrintSuccessors(node)
	PrintFingers(node)

	/*
		log.Info("Kill alert.\n")

		Wait(2 * time.Minute)
		PrintStorage(node)
		PrintPredecessor(node)
		PrintSuccessors(node)
		PrintFingers(node)

		Wait(time.Minute)
		PrintStorage(node)
		PrintPredecessor(node)
		PrintSuccessors(node)
		PrintFingers(node)
	*/

	Wait(time.Minute)

	if node.Stop() != nil {
		log.Fatal(err.Error() + "TERROR\n")
		return
	}
}
