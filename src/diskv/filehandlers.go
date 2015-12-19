package diskv

import (
	"encoding/gob"
	"os"
)
import "shardmaster"
import "io/ioutil"
import "log"

import "bytes"


type State struct {
	config shardmaster.Config
	seq    int
	index  int
	me     string
}


func (kv *DisKV) encodeState(state State) string {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	
	enc.Encode(state.config)
	enc.Encode(state.seq)
	enc.Encode(state.index)
	enc.Encode(state.me)
	
	return string(buf.Bytes())
}

func (kv *DisKV) decodeState(buf string) State {
	reader := bytes.NewBuffer([]byte(buf))
	dec := gob.NewDecoder(reader)
	
	var state State
	
	dec.Decode(&state.config)
	dec.Decode(&state.seq)
	dec.Decode(&state.index)
	dec.Decode(&state.me)
	
	return state
} 

func (kv *DisKV) stateDirPath()	string {
	dir := kv.dir + "/state/"
	
	_, err := os.Stat(dir)
	
	if err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", dir, err)
		}
	}
	
	return dir
}

func (kv *DisKV) readState() error {
	dir := kv.stateDirPath() + "state"
	data, err := ioutil.ReadFile(dir)
	
	if err == nil {
		state := kv.decodeState(string(data))
		kv.config = state.config
		kv.index  = state.index
		kv.clientid     = state.me
		kv.seq    = state.seq	
	} else {
		return err
	}	
	return nil
}

func (kv *DisKV) writeState() error {
	tempdir := kv.stateDirPath() + "tempstate"
	dir := kv.stateDirPath() + "state"
	
	state := State {
		config : kv.config,
		index  : kv.index,
		me     : kv.clientid,
		seq    : kv.seq,
	}
	
	data := kv.encodeState(state)
	
	if err := ioutil.WriteFile(tempdir, []byte(data), 0666); err != nil {
		return err
	}
	
	//we do this renaming so that if there is a crash while writing the file, it would not be considered. This 
	// provides better atomicity. Hint provided by the professor.
	if err := os.Rename(tempdir, dir); err != nil {
		return err
	}
	
	return nil
}


func (kv *DisKV) readDatabase() map[string]string {
	database := map[string]string{}
	
	for shard := 0; shard < shardmaster.NShards; shard++ {
		shardData := kv.fileReadShard(shard)
		
		for k, v := range shardData {
			database[k] = v
		}
	}
	
	return database
}


func (kv *DisKV) writeDatabase(database map[string]string) error {
	for k, v := range database {
		for {
			err := kv.filePut(key2shard(k), k, v)
			if (err == nil) {
				break
			}
		}
	}	
	return nil
}


func (kv *DisKV) logsDirPath()	string {
	dir := kv.dir + "/log/"
	
	_, err := os.Stat(dir)
	
	if err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", dir, err)
		}
	}
	
	return dir
}

func (kv *DisKV) fileLogGet(key string) (string, error) {
	fullname := kv.logsDirPath() + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

func (kv *DisKV) fileLogPut(key string, content string) error {
	fullname := kv.logsDirPath() + "/key-" + kv.encodeKey(key)
	tempname := kv.logsDirPath() + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}


func (kv *DisKV) readLogs() map[string]string {
	logs := map[string]string{}
	dir := kv.logsDirPath()
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatalf("readLogs could not read %v: %v", dir, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("readLogs bad file name %v: %v", n1, err)
			}
			content, err := kv.fileLogGet(key)
			if err != nil {
				log.Fatalf("readLogs fileGet failed for %v: %v", key, err)
			}
			logs[key] = content
		}
	}
	return logs
}

func (kv *DisKV) writeLogs(logs map[string]string) error {
	for k,v := range logs {
		for {
			err := kv.fileLogPut(k, v)
			if err == nil {
				break
			}
		}
	}
	return nil
}

func (kv *DisKV) writeLogsLoop(key string, content string) error {
	for {
		err := kv.fileLogPut(key, content)
		if (err == nil) {
			break
		}
	}
	return nil
}

func (kv *DisKV) writeDatabaseLoop(shard int, key string, content string) error {
	for {
		err := kv.filePut(shard, key, content)
		if (err == nil) {
			break
		}
	}
	return nil
}

func (kv *DisKV) writeStateLoop() error {
	for {
		err := kv.writeState()
		if (err == nil) {
			break
		}
	}
	return nil
}

func (kv *DisKV) isLostDisk() bool {
	dir := kv.dir
	files, _ := ioutil.ReadDir(dir)
	
	DPrintf("", kv.me)
	DPrintf("lost:", files)
	
	if (len(files) == 0) {
		return true
	} else {
		return false
	}
}