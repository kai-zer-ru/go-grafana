package grafana

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	statChannels     = make(chan statData, 1000000) // канал сбора статистики
	StopChannel      = make(chan int)
	lockStat         sync.Mutex
	statCounters     = map[uint32]int64{} // данные по счетчикам
	nextStatSaveTime int64
)

type (
	Grafana struct {
		Address              string
		conn                 net.Conn
		isConnected          bool
		StatSaveSecondPeriod int64
		StatKeyNames         map[uint32]string  // имена счетчиков в Cacti
		StatKeyMultipliers   map[uint32]float64 // множители для счетчиков
		DaemonName           string
	}
	statData struct {
		statType      uint32
		counter       int64
		statType2     uint32
		counter2      int64
		saveValueType uint16 // 0 - суммирование счетчика, 1 - текущее значение, 2 - максимум, 3 - минимум
	}
)

func (g *Grafana) Init() error {
	if g.isConnected {
		return nil
	}
	if g.Address == "" {
		return errors.New("address is empty")
	}
	var err error
	g.conn, err = net.Dial("tcp", g.Address)
	if err != nil {
		return err
	}
	g.isConnected = true
	g.StatKeyNames = make(map[uint32]string)
	g.StatKeyMultipliers = make(map[uint32]float64)
	return nil
}

func (g *Grafana) CloseConnection() error {
	if g.isConnected {
		return g.conn.Close()
	}
	return nil
}

func (g *Grafana) send(metric string, value uint32) error {
	if g.isConnected {
		_, err := fmt.Fprintf(g.conn, fmt.Sprintf("%s %v %v\n", metric, value, time.Now().Unix()))
		return err
	}
	return nil
}

func (g *Grafana) SendValueStatData(statType uint32, value int64, saveValueType uint16) {
	statChannels <- statData{statType: statType, counter: value, saveValueType: saveValueType}
}

func (g *Grafana) HandlerStat() {
	t := time.Now().Unix()
	nextStatSaveTime = t + g.StatSaveSecondPeriod - t%g.StatSaveSecondPeriod
	for {
		select {
		case <-StopChannel:
			_ = g.CloseConnection()
			return
		case data := <-statChannels:
			lockStat.Lock()
			if data.statType > 0 {
				g.addToStatCounter(data.statType, data.counter, data.saveValueType)
			}

			if data.statType2 > 0 {
				g.addToStatCounter(data.statType2, data.counter2, data.saveValueType)
			}
			lockStat.Unlock()
		case <-time.After(time.Duration(nextStatSaveTime-time.Now().Unix()) * time.Second):
			g.saveStat()
			continue
		}

		if time.Now().Unix() >= nextStatSaveTime {
			g.saveStat()
		}
	}
}

func (g *Grafana) addToStatCounter(statType uint32, counter int64, saveValueType uint16) {
	d, ok := statCounters[statType]
	if !ok {
		statCounters[statType] = counter
	} else if saveValueType == 0 { // статистика как счетчик
		statCounters[statType] = d + counter
	} else if saveValueType == 1 { // статистика как значение
		statCounters[statType] = counter
	} else if saveValueType == 2 { // статистика как максимум значения
		if d < counter {
			statCounters[statType] = counter
		}
	}
}

func (g *Grafana) saveStat() {
	t := time.Now().Unix()
	lockStat.Lock()
	for k, v := range statCounters {
		if m, ok := g.StatKeyMultipliers[k]; ok {
			v = int64(float64(v) * m)
		}
		if n, ok := g.StatKeyNames[k]; ok {
			_ = g.send(fmt.Sprintf("Daemon.%s.%s", g.DaemonName, n), uint32(v))
		} else if k < 100 {
			_ = g.send(fmt.Sprintf("Daemon.%s.%d", g.DaemonName, k), uint32(v))
		}
	}
	_ = g.send(fmt.Sprintf("Daemon.%s.%s", g.DaemonName, "IsRunning"), 1)
	nextStatSaveTime = t + g.StatSaveSecondPeriod - t%g.StatSaveSecondPeriod
	statCounters = map[uint32]int64{}
	lockStat.Unlock()
}
