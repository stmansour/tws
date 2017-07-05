package tws

// Timed Work Scheduler - a task scheduling subsystem that supports tasks that
// need to run at a particular time. Tasks can run once or many times by
// rescheduling the same task for a later time. Handler routines for tasks
// must be registered prior to inserting tasks into the scheduler queue.
// Attempts to insert a task with an unregistered handler will fail with an
// error.
import (
	"bytes"
	"database/sql"
	"extres"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

// AppConfig is the shared struct of configuration values
var AppConfig extres.ExternalResources

// Item is the structure of data defining
// a timed-work-scheduled-item of work.
type Item struct {
	TWSID        int64
	Owner        string    // caller identifier
	OwnerData    string    // data managed by the worker
	WorkerName   string    // the work function that will handle this work item
	ActivateTime time.Time // when this time occurs or passes, this work item needs to be launched
	Node         string    // which node in the multi-node
	FLAGS        uint64    // 1<<0 = worker called, 1<<1 = worker ack, 1<<2 rescheduled
	DtActivated  time.Time // when was this item last activated
	DtCompleted  time.Time // when did the work complete
	DtCreate     time.Time // when this item was created
	DtLastUpdate time.Time // last time we heard from the Worker() routine
}

// GridItem is grid display version of Item
type GridItem struct {
	TWSID         int64
	Owner         string // caller identifier
	OwnerData     string // data managed by the worker
	WorkerName    string // the work function that will handle this work item
	ActivateTime  string // when this time occurs or passes, this work item needs to be launched
	RemainingTime string // duration from now til activation
	Node          string // which node in the multi-node
	FLAGS         uint64 // 1<<0 = worker called, 1<<1 = worker ack, 1<<2 rescheduled
	DtActivated   string // when was this item last activated
	DtCompleted   string // when did the work complete
	DtCreate      string // when this item was created
	DtLastUpdate  string // last time we heard from the Worker() routine
	Recid         int64  `json:"recid"`
}

// All callback functions need to be registered so that we can survive a crash, restart, etc.

type worker struct {
	Name     string
	Function func(*Item)
}

// TableGrid is a representation of the
type TableGrid struct {
	Status  string     `json:"status"`
	Total   int64      `json:"total"`
	Records []GridItem `json:"records"`
}

var registry = map[string]worker{}

// PreparedStatements holds the prepared statements for this package
type PreparedStatements struct {
	GetTWS     *sql.Stmt
	Ready      *sql.Stmt
	InsertItem *sql.Stmt
	UpdateItem *sql.Stmt
	DeleteItem *sql.Stmt
	FindItem   *sql.Stmt
	GetAll     *sql.Stmt
}

// TWSctx is a context struct for this package
var TWSctx struct {
	Db       *sql.DB            // db with TWS table
	DBdir    *sql.DB            // directory database
	Zone     *time.Location     // timezone for printing
	Node     string             // unique identifying string for this running instance
	Prepstmt PreparedStatements // sql statements needed by this package
}

// Init initializes the TWS subsystem
func Init(db, dir *sql.DB) {
	TWSctx.Db = db
	TWSctx.DBdir = dir

	cmd := exec.Command("/bin/hostname", "-f")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		e := fmt.Errorf("FATAL error: could not run /bin/hostname")
		log.Fatalf("%s\n", e.Error())
	}
	TWSctx.Node = strings.TrimSpace(out.String())
	err = CreatePreparedStatements()
	if err != nil {
		e := fmt.Errorf("FATAL error with CreatePreparedStatements: %s", err.Error())
		log.Fatalf("%s\n", e.Error())
	}
	TWSctx.Zone = time.Local
	go Scheduler()
}

// RegisterWorker adds a worker function definition to the registry
func RegisterWorker(name string, f func(*Item)) error {
	_, ok := registry[name]
	if ok {
		return fmt.Errorf("function with name %q is already registered", name)
	}
	w := worker{Name: name, Function: f}
	registry[name] = w
	return nil
}

// CreatePreparedStatements builds the sql statements used by this package
func CreatePreparedStatements() error {
	var flds string
	var err error
	flds = "TWSID,Owner,OwnerData,WorkerName,ActivateTime,Node,FLAGS,DtActivated,DtCompleted,DtCreate,DtLastUpdate"
	TWSctx.Prepstmt.GetTWS, err = TWSctx.Db.Prepare("SELECT " + flds + " FROM TWS WHERE TWSID=?")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.Ready, err = TWSctx.Db.Prepare("SELECT " + flds + " FROM TWS WHERE Node=? AND ActivateTime <= ?")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.GetAll, err = TWSctx.Db.Prepare("SELECT " + flds + " FROM TWS ORDER BY ActivateTime DESC LIMIT ? OFFSET ?")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.FindItem, err = TWSctx.Db.Prepare("SELECT " + flds + " FROM TWS WHERE Node=? AND Owner=?")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.InsertItem, err = TWSctx.Db.Prepare("INSERT INTO TWS (Owner,OwnerData,WorkerName,ActivateTime,Node,FLAGS,DtActivated,DtCompleted) VALUES(?,?,?,?,?,?,?,?)")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.UpdateItem, err = TWSctx.Db.Prepare("UPDATE TWS SET Owner=?,OwnerData=?,WorkerName=?,ActivateTime=?,FLAGS=?,DtActivated=?,DtCompleted=? WHERE TWSID=?")
	if err != nil {
		return err
	}
	TWSctx.Prepstmt.DeleteItem, err = TWSctx.Db.Prepare("DELETE from TWS WHERE TWSID=?")
	if err != nil {
		return err
	}
	return nil
}

// InsertItem adds a new work item to the scheduler
func InsertItem(a *Item) error {
	_, ok := registry[a.WorkerName]
	if !ok {
		return fmt.Errorf("No registered worker named: %s", a.WorkerName)
	}
	rid := int64(0)
	a.Node = TWSctx.Node
	res, err := TWSctx.Prepstmt.InsertItem.Exec(a.Owner, a.OwnerData, a.WorkerName, a.ActivateTime, a.Node, a.FLAGS, a.DtActivated, a.DtCompleted)
	if nil == err {
		id, err := res.LastInsertId()
		if err == nil {
			rid = int64(id)
			a.TWSID = rid
		}
	} else {
		log.Print(fmt.Sprintf("InsertItem: error inserting Item:  %s\n", err.Error()))
	}
	return err
}

// UpdateItem is used to update the status of a scheduled task
func UpdateItem(a *Item) error {
	_, ok := registry[a.WorkerName]
	if !ok {
		return fmt.Errorf("No registered worker named: %s", a.WorkerName)
	}
	_, err := TWSctx.Prepstmt.UpdateItem.Exec(a.Owner, a.OwnerData, a.WorkerName, a.ActivateTime, a.FLAGS, a.DtActivated, a.DtCompleted, a.TWSID)
	return err
}

func readTWSItems(rows *sql.Rows, a *Item) {
	rows.Scan(&a.TWSID, &a.Owner, &a.OwnerData, &a.WorkerName, &a.ActivateTime, &a.Node, &a.FLAGS, &a.DtActivated, &a.DtCompleted, &a.DtCreate, &a.DtLastUpdate)
}

// WSGridData returns a version of the TWS table that is suitable for putting
// into a w2ui grid.
func WSGridData(limit, offset int) (TableGrid, error) {
	var m TableGrid
	WSGridTimeFmt := "15:04:05 MST  Jan _2, 2006"
	rows, err := TWSctx.Prepstmt.GetAll.Query(limit, offset)
	if err != nil {
		return m, err
	}
	defer rows.Close()
	now := time.Now()
	for rows.Next() {
		var a Item
		readTWSItems(rows, &a)
		remaining := "past"
		if a.ActivateTime.After(now) {
			dur := a.ActivateTime.Sub(now)
			remaining = dur.String()
		}
		var b = GridItem{
			TWSID:         a.TWSID,
			Owner:         a.Owner,
			OwnerData:     a.OwnerData,
			WorkerName:    a.WorkerName,
			ActivateTime:  a.ActivateTime.In(TWSctx.Zone).Format(WSGridTimeFmt),
			RemainingTime: remaining,
			Node:          a.Node,
			FLAGS:         a.FLAGS,
			DtActivated:   a.DtActivated.In(TWSctx.Zone).Format(WSGridTimeFmt),
			DtCompleted:   a.DtCompleted.In(TWSctx.Zone).Format(WSGridTimeFmt),
			DtCreate:      a.DtCreate.In(TWSctx.Zone).Format(WSGridTimeFmt),
			DtLastUpdate:  a.DtLastUpdate.In(TWSctx.Zone).Format(WSGridTimeFmt),
			Recid:         a.TWSID,
		}
		m.Records = append(m.Records, b)
	}
	m.Total = int64(len(m.Records))
	m.Status = "success "
	return m, nil
}

// ItemWorking marks that this item is working.
func ItemWorking(a *Item) error {
	a.FLAGS &= ^(uint64(1 << 2)) // zero out bit 2, the caller just started on this. It is no longer rescheduled
	a.FLAGS |= 3                 // worker acknowledged receipt
	return UpdateItem(a)
}

// RescheduleItem marks this item as rescheduled for the supplied time
func RescheduleItem(a *Item, t time.Time) error {
	a.FLAGS &= 0xfffffffffffffffc // active=0, worker ack=0, rescheduled=1
	a.FLAGS |= 4                  // set the rescheduled flag
	a.ActivateTime = t
	a.DtCompleted = time.Now()
	return UpdateItem(a)
}

// CompleteItem removes an item from the work queue
func CompleteItem(a *Item) error {
	return DeleteItem(a.TWSID)
}

// FindItem returns all items associated with the supplied owner string, o
func FindItem(o string) ([]Item, error) {
	var m []Item
	rows, err := TWSctx.Prepstmt.FindItem.Query(TWSctx.Node, o)
	if err != nil {
		return m, err
	}
	defer rows.Close()
	for rows.Next() {
		var a Item
		readTWSItems(rows, &a)
		m = append(m, a)
	}
	return m, nil
}

// DeleteItem is used to remove a Item from the Schedulers
// work list
func DeleteItem(id int64) error {
	_, err := TWSctx.Prepstmt.DeleteItem.Exec(id)
	return err
}

// LaunchTimedWork spins through the work table for work items whose times have arrive
func LaunchTimedWork() error {
	rows, err := TWSctx.Prepstmt.Ready.Query(TWSctx.Node, time.Now())
	if err != nil {
		return err
	}
	defer rows.Close()
	now := time.Now()
	for rows.Next() {
		var a Item
		readTWSItems(rows, &a)
		if now.After(a.ActivateTime) {
			a.DtActivated = time.Now()
			a.DtCompleted = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
			UpdateItem(&a)
			if nil == registry[a.WorkerName].Function {
				continue
			}
			go registry[a.WorkerName].Function(&a)
		}
	}
	return nil
}

// Scheduler is the main go routine for this system. It
func Scheduler() {
	numSecs := time.Duration(10)
	chkTime := numSecs * time.Second
	for {
		select {
		case <-time.After(chkTime):
			LaunchTimedWork()
		}
	}
}
