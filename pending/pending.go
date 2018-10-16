package pending

import (
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/borud/sst/logpending"
	"github.com/golang/protobuf/proto"
)

type Pending struct {
	logFile      *os.File
	state        map[int64]*logpending.Entry
	pulseChan    chan bool
	pulseDelay   time.Duration
	unsyncCount  int64
	unsyncMax    int64
	lastSyncTime time.Time
}

const defaultPulseDelay time.Duration = time.Microsecond * 10000

func New(filename string) (*Pending, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	p := &Pending{
		logFile:    f,
		pulseChan:  make(chan bool),
		pulseDelay: defaultPulseDelay,
		state:      make(map[int64]*logpending.Entry),
		unsyncMax:  10000,
	}

	go p.pulse()

	return p, nil
}

func (p *Pending) Close() error {
	return p.logFile.Close()
}

func (p *Pending) Add(id int64, ts time.Time, try int32, payload []byte) error {
	e := &logpending.Entry{
		Id:        id,
		Ts:        ts.Unix(),
		Try:       try,
		Payload:   payload,
		Operation: logpending.OperationType_Add,
	}

	// Add to state map
	p.state[id] = e

	buf, err := proto.Marshal(e)
	if err != nil {
		return err
	}

	_, err = p.logFile.Write(buf)
	if err != nil {
		return err
	}

	atomic.AddInt64(&p.unsyncCount, 1)
	p.maybeSync()

	return nil
}

func (p *Pending) Commit(id int64, ts time.Time) error {
	e := &logpending.Entry{
		Id:        id,
		Ts:        ts.Unix(),
		Operation: logpending.OperationType_Commit,
	}

	// Remove from state map
	delete(p.state, id)

	buf, err := proto.Marshal(e)
	if err != nil {
		return err
	}

	_, err = p.logFile.Write(buf)
	if err != nil {
		return err
	}

	atomic.AddInt64(&p.unsyncCount, 1)
	p.maybeSync()

	return nil
}

func (p *Pending) pulse() {
	for {
		time.Sleep(p.pulseDelay)

		// If we have recently written to disk we can wait another delay
		if time.Now().Sub(p.lastSyncTime) < p.pulseDelay {
			continue
		}

		log.Print("Pulse")
		p.pulseChan <- true
	}
}

func (p *Pending) maybeSync() {
	doSync := false

	select {
	case _ = <-p.pulseChan:
		doSync = true

	default:
		if p.unsyncCount >= p.unsyncMax {
			doSync = true
		}
	}

	if doSync {
		p.logFile.Sync()
		c := atomic.LoadInt64(&p.unsyncCount)
		atomic.StoreInt64(&p.unsyncCount, 0)
		p.lastSyncTime = time.Now()
		log.Printf("Sync = %d", c)
	}
}
