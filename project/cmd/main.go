package main

import (
	"archive/zip"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"gopkg.in/yaml.v2"
)

type Configuration struct {
	InboundDirectories  []string            `yaml:"inbound_directories"`
	RegexToDestinations []RegexDestinations `yaml:"regex_to_destinations"`
	SSHKeyPath          string              `yaml:"ssh_key_path"`
	ConcurrencyLimits   ConcurrencySettings `yaml:"concurrency_limits"`
	ErrorCodes          ErrorCodeSettings   `yaml:"error_codes"`
	Directories         DirectoryPaths      `yaml:"directories"`
}
type RegexDestinations struct {
	Pattern      string        `yaml:"pattern"`
	Destinations []Destination `yaml:"destinations"`
}
type Destination struct {
	Host       string `yaml:"host"`
	UserName   string `yaml:"username"`
	RemotePath string `yaml:"remote_path"`
}
type ConcurrencySettings struct {
	Default struct {
		LargeFileLimit int `yaml:"large_file_limit"`
		SmallFileLimit int `yaml:"small_file_limit"`
	} `yaml:"default"`
	ServerOverrides map[string]map[string]int `yaml:"server_overrides"`
}
type ErrorCodeSettings struct {
	FileErrors     map[string]int `yaml:"file_errors"`
	DatabaseErrors map[string]int `yaml:"database_errors"`
	TransferErrors map[string]int `yaml:"transfer_errors"`
	ConfigErrors   map[string]int `yaml:"config_errors"`
	NetworkErrors  map[string]int `yaml:"network_errors"`
	MetricsErrors  map[string]int `yaml:"metrics_errors"`
}
type DirectoryPaths struct { // New type
	Staging string `yaml:"staging"`
	Error   string `yaml:"error"`
}
type FileMeta struct {
	Name           string
	Size           int64
	DateCreated    time.Time
	DateModified   time.Time
	TransferStatus string
}

const dbFile = "flytblast.db"

// LoadConfig loads configuration data from the YAML file
func LoadConfig(filename string) Configuration {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalf("ERROR - {unknown} Failed to read config file: %v", err)
	}
	var config Configuration
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Fatalf("ERROR - {unknown} Failed to parse config file: %v", err)
	}
	return config
}

// InitDatabase initializes the SQLite database
func InitDatabase() *sql.DB {
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatalf("ERROR - {311} Failed to open database: %v", err)
	}
	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS file_logs (
            id INTEGER PRIMARY KEY,
            filename TEXT UNIQUE,
            size INTEGER,
            date_created TIMESTAMP,
            date_modified TIMESTAMP,
            transfer_status TEXT
        );
    `)
	if err != nil {
		log.Fatalf("ERROR - {312} Failed to create table: %v", err)
	}
	return db
}

// RecordFileMetadata inserts or updates file records in the database
func RecordFileMetadata(db *sql.DB, meta FileMeta) error {
	_, err := db.Exec(`
        INSERT OR REPLACE INTO file_logs (filename, size, date_created, date_modified, transfer_status)
        VALUES (?, ?, ?, ?, ?)`,
		meta.Name, meta.Size, meta.DateCreated, meta.DateModified, meta.TransferStatus,
	)
	if err != nil {
		log.Printf("ERROR - {314} Database write failure: %v", err)
	}
	return err
}

// ProcessFiles handles the file processing and transfer
func ProcessFiles(config Configuration, db *sql.DB, statsd *statsd.Client) error {
	for _, dir := range config.InboundDirectories {
		log.Printf("INFO - {101} Watching directory: %s", dir)
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("ERROR - {%d} Failed to read directory: %w", config.ErrorCodes.FileErrors["directory_not_found"], err)
		}
		for _, entry := range files {
			if !entry.IsDir() {
				filename := entry.Name()
				meta, err := GetFileMetadata(filepath.Join(dir, filename), config)
				if err != nil {
					log.Printf("ERROR - {%d} Failed to get metadata for %s: %v", config.ErrorCodes.FileErrors["general"], filename, err)
					continue
				}
				if CheckForDuplicate(db, filename, config) {
					log.Printf("ERROR - {%d} Duplicate file detected: %s", config.ErrorCodes.FileErrors["duplicate_detected"], filename)
					MoveToErrorDirectory(config, filename)
					continue
				}
				log.Printf("INFO - {200} Processing file: %s", filename)
				meta.TransferStatus = "Processing"
				if err := RecordFileMetadata(db, meta); err != nil {
					log.Printf("ERROR - {%d} Failed to record metadata: %v", config.ErrorCodes.DatabaseErrors["write_failure"], err)
				}
				log.Printf("DEBUG - {401} File metadata: %s, Size: %d", meta.Name, meta.Size)
				if CheckFileCorruption(filepath.Join(dir, filename)) {
					log.Printf("ERROR - {%d} File corruption detected: %s", config.ErrorCodes.FileErrors["corruption_detected"], filename)
					MoveToErrorDirectory(config, filename)
					continue
				}
				for _, regexDest := range config.RegexToDestinations {
					match, _ := regexp.MatchString(regexDest.Pattern, filename)
					if match {
						stagedPath := StageFile(config, dir, filename)
						log.Printf("DEBUG - {402} Staged file: %s. Starting transfer process.", stagedPath)
						if err := TransferWithLimits(config, regexDest.Destinations, meta, stagedPath, statsd); err != nil {
							log.Printf("ERROR - {%d} Transfer failed for file %s: %v", config.ErrorCodes.TransferErrors["general"], filename, err)
							meta.TransferStatus = "Transfer Failed"
							MoveToErrorDirectory(config, filename)
						} else {
							meta.TransferStatus = "Transferred"
						}
						if err := RecordFileMetadata(db, meta); err != nil {
							log.Printf("ERROR - {%d} Failed to update metadata: %v", config.ErrorCodes.DatabaseErrors["write_failure"], err)
						}
						break
					}
				}
			}
		}
	}
	return nil
}

// CheckForDuplicate checks if the file is already processed
func CheckForDuplicate(db *sql.DB, filename string, config Configuration) bool {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM file_logs WHERE filename = ?", filename).Scan(&count)
	if err != nil {
		log.Printf("ERROR - {%d} Failed to check duplicates: %v", config.ErrorCodes.DatabaseErrors["execution_failed"], err)
	}
	return count > 0
}

// GetFileMetadata retrieves metadata for the given file
func GetFileMetadata(path string, config Configuration) (FileMeta, error) {
	info, err := os.Stat(path)
	if err != nil {
		return FileMeta{}, fmt.Errorf("ERROR - {%d} Failed to get file info: %w", config.ErrorCodes.FileErrors["general"], err)
	}
	return FileMeta{
		Name:           info.Name(),
		Size:           info.Size(),
		DateCreated:    info.ModTime(),
		DateModified:   info.ModTime(),
		TransferStatus: "New",
	}, nil
}

// CheckFileCorruption placeholder for file integrity check
func CheckFileCorruption(filePath string) bool {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return true
	}
	//Check for empty files
	if fileInfo.Size() == 0 {
		return true
	}
	//checking for the extension
	if filepath.Ext(filePath) == ".tmp" || filepath.Ext(filePath) == ".temp" {
		return true
	}
	if !isFileOpenable(filePath) {
		return true
	}
	return false
}

func isFileOpenable(filePath string) bool {
	zipReader, err := zip.OpenReader(filePath)
	if err != nil {
		return false
	}
	defer zipReader.Close()
	return true
}

// StageFile stages files for processing in a temporary structure
func StageFile(config Configuration, directory, filename string) string {
	stagingDir := config.Directories.Staging
	if _, err := os.Stat(stagingDir); os.IsNotExist(err) {
		os.Mkdir(stagingDir, 0755)
	}
	stagedPath := filepath.Join(stagingDir, filename)
	inputPath := filepath.Join(directory, filename)
	if err := os.Rename(inputPath, stagedPath); err != nil {
		log.Printf("ERROR - {%d} Failed to stage file: %v", config.ErrorCodes.FileErrors["rename_failed"], err)
	}
	return stagedPath
}

// TransferWithLimits determines concurrency limits and handles file transfers
func TransferWithLimits(config Configuration, destinations []Destination, meta FileMeta, filepath string, statsd *statsd.Client) error {
	var wg sync.WaitGroup
	largeFileLimit := config.ConcurrencyLimits.Default.LargeFileLimit
	smallFileLimit := config.ConcurrencyLimits.Default.SmallFileLimit
	for _, dest := range destinations {
		if serverLimits, ok := config.ConcurrencyLimits.ServerOverrides[dest.Host]; ok {
			if meta.Size > 1*1024*1024 && serverLimits["large_file_limit"] != 0 {
				largeFileLimit = serverLimits["large_file_limit"]
			}
			if meta.Size <= 1*1024*1024 && serverLimits["small_file_limit"] != 0 {
				smallFileLimit = serverLimits["small_file_limit"]
			}
		}
		limit := largeFileLimit
		if meta.Size <= 1*1024*1024 {
			limit = smallFileLimit
		}
		ch := make(chan struct{}, limit)
		for i := 0; i < limit; i++ {
			ch <- struct{}{}
		}
		for _, dest := range destinations {
			dest := dest
			wg.Add(1)
			go func() {
				defer func() {
					<-ch
					wg.Done()
				}()
				if err := TransferFile(filepath, dest, config.SSHKeyPath, config); err != nil {
					log.Printf("ERROR - {%d} Transfer failed for %s to %s: %v", config.ErrorCodes.TransferErrors["transfer_interrupted"], filepath, dest.Host, err)
					return
				}
				log.Printf("DEBUG - {403} %s transferred to %s on %s.", filepath, dest.RemotePath, dest.Host)
				statsd.Incr("flytblast.files_transferred", []string{}, 1.0)
			}()
		}
		wg.Wait()
	}
	return nil
}

// TransferFile transfers a single file via SSH
func TransferFile(filepath string, dest Destination, sshKeyPath string, config Configuration) error {
	clientConfig, err := NewSSHClientConfig(dest.UserName, sshKeyPath, config)
	if err != nil {
		return fmt.Errorf("ERROR - {%d} Failed to configure SSH: %w", config.ErrorCodes.TransferErrors["auth_failed"], err)
	}
	client, err := ssh.Dial("tcp", dest.Host+":22", clientConfig)
	if err != nil {
		return fmt.Errorf("ERROR - {%d} Failed to dial SSH: %w", config.ErrorCodes.TransferErrors["ssh_connection_failed"], err)
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("ERROR - {%d} Failed to create session: %w", config.ErrorCodes.TransferErrors["command_execution_failed"], err)
	}
	defer session.Close()
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("ERROR - {%d} Failed to open file: %w", config.ErrorCodes.FileErrors["general"], err)
	}
	defer file.Close()
	session.Stdin = file
	remoteFilePath := fmt.Sprintf("%s/%s", dest.RemotePath, filepath)
	if err := session.Run(fmt.Sprintf("cat > %s", remoteFilePath)); err != nil {
		return fmt.Errorf("ERROR - {%d} Failed to run remote command: %w", config.ErrorCodes.TransferErrors["command_execution_failed"], err)
	}
	return nil
}

// NewSSHClientConfig generates an SSH client configuration using a private key
func NewSSHClientConfig(username, keyPath string, config Configuration) (*ssh.ClientConfig, error) {
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("ERROR - {%d} Failed to read key: %w", config.ErrorCodes.FileErrors["general"], err)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("ERROR - {%d} Failed to parse key: %w", config.ErrorCodes.FileErrors["general"], err)
	}
	hostKeyCallback, err := knownhosts.New("")
	if err != nil {
		return nil, fmt.Errorf("ERROR - {%d} Failed to configure host key callback: %w", config.ErrorCodes.TransferErrors["host_key_verification_failed"], err)
	}
	return &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
	}, nil
}

// MoveToErrorDirectory moves a file to an error directory for inspection
func MoveToErrorDirectory(config Configuration, filename string) {
	errorDir := config.Directories.Error
	if _, err := os.Stat(errorDir); os.IsNotExist(err) {
		os.Mkdir(errorDir, 0755)
	}
	srcPath := filepath.Join(config.Directories.Staging, filename)
	dstPath := filepath.Join(errorDir, filename)
	if err := os.Rename(srcPath, dstPath); err != nil {
		log.Printf("ERROR - {%d} Failed to move file %s to error directory: %v", config.ErrorCodes.FileErrors["rename_failed"], filename, err)
	}
}

func main() {
	config := LoadConfig("project/config/config.yaml")
	db := InitDatabase()
	defer db.Close()
	statsdClient, err := statsd.New("localhost:8125")
	if err != nil {
		log.Fatalf("ERROR - {%d} Failed to initialize statsd: %v", config.ErrorCodes.MetricsErrors["statsd_init_failed"], err)
	}
	defer statsdClient.Close()
	if err := ProcessFiles(config, db, statsdClient); err != nil {
		log.Printf("ERROR - {%d} Error processing files: %v", config.ErrorCodes.FileErrors["general"], err)
	}
}
