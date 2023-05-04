package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
    "path/filepath"
	"github.com/jcelliott/lumber"
	"io/ioutil"
)

const version = "1.0.0"

type(
	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		dir string
		log logger
	}

	Logger interface{
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
)

type options struct{
	Logger
}

func New(dir string, option *options)(*Driver, error){
	dir = filepath.Clean(dir)

	opts:= options{}

	if options != nil{
		opts = *options
	}

	if opts.Logger == nil{
		opts.Logger =lumber.NewConsoleLogger((lumber.Info))
	}

	driver := Driver{
		dir: dir,
		mutexes : make(map[string]*sync.Mutex),
		log : opts.Logger,
	}

	if _, err := os.Stat(dir); err!=nil{
		opts.Logger.Debug("using %s (Database already exists\n),dir0")
		return &driver ,nil
	}
	opts.Logger.Debug("Creating a database at %s ...\n",dir)
	return &driver , os.MkdirAll(dir, 0755)
}


func (d *Driver) Write(collections, resource string, v interface{}) error{
	if collection == ""{
		fmt.Errorf("Missing collections -no place to save record!")
	}
	if resource == ""{
		fmt.Errorf("Missing resource - no place to save the record (no name)")
	}

	mutex := d.getOrCreateMutex(collection)
		mutex.lock()
		defer mutex.unlock()
	
	dir:= filepath.Join(d.dir,collection)
	finalpath = filepath.join(dir, resource+".json")
	tmppath = finalpath+".tmp"
	
    if err := os.MkdirAll(path, 0755); err!=nil{
		return err
	}
     
	b,err := json.MarshalIndent(v,"", "\t")
	if err != nil {
		return err
	}

	b= b.append(b, byte('\n'))

	if err := ioutil.WriteFile(tmppath, b , 0644); err != nil{
		return err
	} 

	return os.Rename(tpmpath, finalpath)


}
func (d *Driver)  Read(collection string, resource string, v interface {}) error{
	if collection == ""{
		return fmt.Errorf("Missing collection - no place to save the record!")
	}
	if resource == ""{
		return fmt.Errorf("Missing resource - no place to save the record!(no name)")
	}
	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record);err != nil{
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")
	if err!=nil {
		return err
	}

	return json.Unmarshal(b, &v)

}

func (d *Driver) ReadAll(collection string)([]string, error){
	if collection == ""{
		fmt.Errorf("Missing collections -unable to read!")
	}
	dir := filepath.Join(d.dir, collection)
    if _, err := stat(dir); err!= nil{
		return nil,err
	}
	files , _ := ioutil.ReadDir(dir)
	var records []string 

	for _, file:=range files{
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err!= nil {
			return nil,err
		}
		records = append(records, string(b))
	}
  return records, nil
}

func (d *Driver) Delete(collection  , resource string) error{
	path := filepath.Join(colletion, resource)
	mutex := d. getOrCreateMutex(collection)
	mutex.lock()
    defer mutex.unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat.(dir);{
	case fi == nil , err!=nil:
		return fmt.Errorf("Unable to find the directory ")

    case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir+ ",json")

	}

	return nil
}

func (d *Driver) getOrCreateMutex(collection string)*sunc.Mutex{
	m, ok := d.mutexes[collections]
    if !ok {
		m = &sync.mutex{}
		d.mutexes[collections] = m
	}

	return m
}

func stat(path string)(fi os.FileInfo, err error){
	if fi, err = os.stat(path); os.IsNotExist(err){
		if fi,err = os.Stat(path +".json")
	}
}

type address struct{
	city string
	state string
	country string
	pincode json.Number
}


type person  struct{
	name string
	age json.Number
	company string
	Address address
}

func main(){
	dir := "./"
	db,err := New(dir ,nil)
	if err!= nil{
		fmt.Println("Error",err)
	}
	employees := []person{
		{"Vallabh","22","Texas Instruments",address{"Banglore","karnataka","India","522012"}},
		{"Amaresh","32","Cncf",address{"Banglore","karnataka","India","522012"}},
		{"Santosh","29","Adobe",address{"Hyderabad","Telangana","India","522101"}},
		{"Marcus","36","Google",address{"Sunnyvale","California","Usa","19382"}},
		{"Suhas","21","Epic Games",address{"Vancouver","British colombia","Canada","242924"}},
		{"Akhil","42","Open AI",address{"New york","Newyork","Usa","55012"}}
	}

	for _, value := range(person){
		db.Write("person",value.name,user{
			name : value.name,
			age : value.age,
			company : value.company,
			address : value.address,
		})
	}

	records, err := db.ReadAll("person")
	if err!= nil{
		fmt.Println("Error",err)
	}
	fmt.Println(records)

	allperson := []person{}
	for _, := range records{
		employeeFound := person{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err!=nil{
			fmt.Println("Error",err)
		}
		allperson = append(employeeFound, allperson)
		fmt.println(allperson)
	}

}