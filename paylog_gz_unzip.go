/*
 支付日志gz文件解压缩脚本
 @author jakquan
 @since 2015/3/30
*/
package main

import (
	"strings"
	"flag"
	"github.com/jimlawless/cfg"
	"bufio"
	"io"
	"compress/gzip"
	"fmt"
	daemon "github.com/xgdapg/daemon"
	"log"
	"os"
	"path/filepath"
	"time"
)

var logger *log.Logger

var logfile *os.File

var cfg_file string

var cfg_map map[string]string

func main() {
	//加载配置
	flag.StringVar( &cfg_file, "conf", "./default.conf", "Input the config file use --conf=[file path]" );
	flag.Parse()
	
	load_cfg( cfg_file )

	init_logger( cfg_map["log_file"] )
 	
	var pid = os.Getpid()

	logger.Println( "config file:", cfg_file  )
	logger.Println( "unzip process start,PID:", pid )	
	//read the gz file
	for {
		files, err := gzipList(cfg_map["input_dir"])
		//fmt.Println( files )
		if err == nil {
			readGz(files)

		} else {
			logger.Println(err)
		}		
		time.Sleep( time.Duration( 10 ) * time.Second)
	}
	
}

func init() {
	daemon.Exec(daemon.Daemon) // send the process to the background
}

func gzipList(dir string) (files []string, err error) {
	var domains_filter []string
	var domains_filter_cfg string
	var gz_files []string
	match := fmt.Sprintf("%s/*.gz", dir)
	gz_files, err = filepath.Glob(match)
	if err != nil {
		logger.Println("list gz files err:",gz_files)
		return
	}
	domains_filter_cfg = cfg_map["domains_filter"]
	domains_filter = strings.Split( domains_filter_cfg, "," )
	for _,fname := range gz_files {
		var in = in_array( domains_filter, filepath.Base(fname) )
		//fmt.Println( "fname:",fname,"in?",in)
		if( in ){
			files = append( files, fname )
			//fmt.Println( "find one", files )
			continue;
		}
	}
	return
}

func in_array( array []string, findme string )(bool){
	for _,v := range array {
		if( strings.Contains( findme, v )){
			return true
		}
	}		
	return false
}

func load_cfg( filepath string ){
	cfg_map = make(map[string]string)
	cfg_err := cfg.Load(filepath, cfg_map)
	if cfg_err != nil {
		fmt.Println("load config file err")
	}
}


func init_logger( log_file string ){
	//日志初始化
	logfile, _ = os.OpenFile( log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	logger = log.New(logfile, "\r\n", log.Ldate|log.Ltime|log.Llongfile)
}

func readGz(files []string) {
	today := time.Now().Format("2006-01-02")
	goadf, err := os.OpenFile(cfg_map["output_dir"]+today+".log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		logger.Println("openfile err:", err)
		return
	}
	defer goadf.Close()

	var br *bufio.Reader

	for i := 0; i < len(files); i++ {
		//logger.Println("find a gz file:"+files[i] + "\n")
		file, fileEr := os.Open(files[i])
		if fileEr != nil {
			logger.Println("file read error:", fileEr)
			return
		}

		defer file.Close()

		fz, gzErr := gzip.NewReader(file)
		if gzErr != nil {
			//解压失败
			br = bufio.NewReader(file)
		} else {
			br = bufio.NewReader(fz)
		}

		for {
			line, err1 := br.ReadString('\n')
			if err1 != nil {
				if err == io.EOF{
					break	
				}
				break
			}
			if 0==len(line) || line=="\n" {
				continue;
			}
			line = strings.Replace(line, "\n", "", -1)
			line = filepath.Base(files[i])+" "+line+"\n"
			_, werr := goadf.WriteString( line )
			if werr != nil {
				logger.Println("writeString err", werr)
			}

		}
	}
	var tt []string
	delGz( tt )
}

func delGz(files []string) {
	var err error
	if( 0==len(files) ){
		var dir = cfg_map["input_dir"]
		match := fmt.Sprintf("%s/*.gz", dir)
	        files, err = filepath.Glob(match)	
		if( err!=nil ){
			logger.Println("清空gz文件目录失败",err)
		}
		//fmt.Println( "files:", files )
	}
	for _, v := range files {
		err := os.Remove(v)
		if err != nil {
			logger.Println("delGz err:", err)
		}
	}
}
