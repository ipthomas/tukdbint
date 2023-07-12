package tukdbint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ipthomas/tukcnst"
)

type DBConnection struct {
	DBUser        string
	DBPassword    string
	DBHost        string
	DBPort        string
	DBName        string
	DBTimeout     string
	DBReadTimeout string
	DB_URL        string
	DEBUG_MODE    bool
}
type Statics struct {
	Action       string   `json:"action"`
	LastInsertId int      `json:"lastinsertid"`
	Count        int      `json:"count"`
	Static       []Static `json:"static"`
}
type Static struct {
	Id      int    `json:"id"`
	Name    string `json:"name"`
	Content string `json:"content"`
}
type Templates struct {
	Action       string     `json:"action"`
	LastInsertId int        `json:"lastinsertid"`
	Count        int        `json:"count"`
	Templates    []Template `json:"templates"`
}
type Template struct {
	Id       int    `json:"id"`
	Name     string `json:"name"`
	Template string `json:"template"`
	User     string `json:"user"`
}
type Subscription struct {
	Id         int    `json:"id"`
	Created    string `json:"created"`
	BrokerRef  string `json:"brokerref"`
	Pathway    string `json:"pathway"`
	Topic      string `json:"topic"`
	Expression string `json:"expression"`
	Email      string `json:"email"`
	NhsId      string `json:"nhsid"`
	User       string `json:"user"`
	Org        string `json:"org"`
	Role       string `json:"role"`
}
type Subscriptions struct {
	Action        string         `json:"action"`
	LastInsertId  int            `json:"lastinsertid"`
	Count         int            `json:"count"`
	Subscriptions []Subscription `json:"subscriptions"`
}
type Event struct {
	Id                 int    `json:"id"`
	Creationtime       string `json:"creationtime"`
	EventType          string `json:"eventtype"`
	DocName            string `json:"docname"`
	ClassCode          string `json:"classcode"`
	ConfCode           string `json:"confcode"`
	FormatCode         string `json:"formatcode"`
	FacilityCode       string `json:"facilitycode"`
	PracticeCode       string `json:"practicecode"`
	Expression         string `json:"expression"`
	Authors            string `json:"authors"`
	XdsPid             string `json:"xdspid"`
	XdsDocEntryUid     string `json:"xdsdocentryuid"`
	RepositoryUniqueId string `json:"repositoryuniqueid"`
	NhsId              string `json:"nhsid"`
	User               string `json:"user"`
	Org                string `json:"org"`
	Role               string `json:"role"`
	Speciality         string `json:"speciality"`
	Topic              string `json:"topic"`
	Pathway            string `json:"pathway"`
	Comments           string `json:"comments"`
	Version            int    `json:"ver"`
	TaskId             int    `json:"taskid"`
	BrokerRef          string `json:"brokerref"`
}
type Events struct {
	Action       string  `json:"action"`
	LastInsertId int     `json:"lastinsertid"`
	Count        int     `json:"count"`
	Events       []Event `json:"events"`
}
type Workflow struct {
	Id        int    `json:"id"`
	Created   string `json:"created"`
	Pathway   string `json:"pathway"`
	NHSId     string `json:"nhsid"`
	XDW_Key   string `json:"xdw_key"`
	XDW_UID   string `json:"xdw_uid"`
	XDW_Doc   string `json:"xdw_doc"`
	XDW_Def   string `json:"xdw_def"`
	Version   int    `json:"version"`
	Published bool   `json:"published"`
	Status    string `json:"status"`
}
type Workflows struct {
	Action       string     `json:"action"`
	LastInsertId int        `json:"lastinsertid"`
	Count        int        `json:"count"`
	Workflows    []Workflow `json:"workflows"`
}
type WorkflowStates struct {
	Action        string          `json:"action"`
	LastInsertId  int             `json:"lastinsertid"`
	Count         int             `json:"count"`
	Workflowstate []Workflowstate `json:"workflowstate"`
}
type Workflowstate struct {
	Id            int    `json:"id"`
	WorkflowId    int    `json:"workflowid"`
	Pathway       string `json:"pathway"`
	NHSId         string `json:"nhsid"`
	Version       int    `json:"version"`
	Published     bool   `json:"published"`
	Created       string `json:"created"`
	CreatedBy     string `json:"createdby"`
	Status        string `json:"status"`
	CompleteBy    string `json:"completeby"`
	LastUpdate    string `json:"lastupdate"`
	Owner         string `json:"owner"`
	Overdue       string `json:"overdue"`
	Escalated     string `json:"escalated"`
	TargetMet     string `json:"targetmet"`
	InProgress    string `json:"inprogress"`
	Duration      string `json:"duration"`
	TimeRemaining string `json:"timeremaining"`
}

type XDWS struct {
	Action       string `json:"action"`
	LastInsertId int    `json:"lastinsertid"`
	Count        int    `json:"count"`
	XDW          []XDW  `json:"xdws"`
}
type XDW struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	IsXDSMeta bool   `json:"isxdsmeta"`
	XDW       string `json:"xdw"`
}
type IdMaps struct {
	Action       string
	LastInsertId int
	Where        string
	Value        string
	Cnt          int
	LidMap       []IdMap
}
type IdMap struct {
	Id   int    `json:"id"`
	User string `json:"user"`
	Lid  string `json:"lid"`
	Mid  string `json:"mid"`
}

var (
	DBConn       *sql.DB
	cachedIDMaps = []IdMap{}
	cached       time.Time
)

// sort interface for events
func (e Events) Len() int {
	return len(e.Events)
}
func (e Events) Less(i, j int) bool {
	return e.Events[i].Id > e.Events[j].Id
}
func (e Events) Swap(i, j int) {
	e.Events[i], e.Events[j] = e.Events[j], e.Events[i]
}

// sort interface for idmaps
func (e IdMaps) Len() int {
	return len(e.LidMap)
}
func (e IdMaps) Less(i, j int) bool {
	return e.LidMap[i].Lid > e.LidMap[j].Lid
}
func (e IdMaps) Swap(i, j int) {
	e.LidMap[i], e.LidMap[j] = e.LidMap[j], e.LidMap[i]
}

// sort interface for Workflows
func (e Workflows) Len() int {
	return len(e.Workflows)
}
func (e Workflows) Less(i, j int) bool {
	return e.Workflows[i].Pathway > e.Workflows[j].Pathway
}
func (e Workflows) Swap(i, j int) {
	e.Workflows[i], e.Workflows[j] = e.Workflows[j], e.Workflows[i]
}

type DBInterface interface {
	newEvent() error
}

func NewDBEvent(i DBInterface) error {
	return i.newEvent()
}

// DBConnection
func CloseDBConnection() {
	if DBConn != nil {
		DBConn.Close()
		log.Println("Closed DB Connection")
	}
}
func (i *DBConnection) newEvent() error {
	var err error
	i.setDBCredentials()
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&timeout=%s&readTimeout=%s",
		i.DBUser,
		i.DBPassword,
		i.DBHost+i.DBPort,
		i.DBName,
		i.DBTimeout,
		i.DBReadTimeout)
	log.Printf("Opening DB Connection to mysql instance User: %s Host: %s Port: %s Name: %s", i.DBUser, i.DBHost, i.DBPort, i.DBName)
	DBConn, err = sql.Open("mysql", dsn)
	if err == nil {
		log.Println("Opened Database")
	}
	return err
}
func (i *DBConnection) setDBCredentials() {
	if i.DBPort == "" {
		i.DBPort = "3306"
	}
	if !strings.HasPrefix(i.DBPort, ":") {
		i.DBPort = ":" + i.DBPort
	}
	if i.DBName == "" {
		i.DBName = "tuk"
	}
	if i.DBTimeout == "" {
		i.DBTimeout = "5"
	}
	if !strings.HasSuffix(i.DBTimeout, "s") {
		i.DBTimeout = i.DBTimeout + "s"
	}
	if i.DBReadTimeout == "" {
		i.DBReadTimeout = "5"
	}
	if !strings.HasSuffix(i.DBReadTimeout, "s") {
		i.DBReadTimeout = i.DBReadTimeout + "s"
	}
}

// Subscriptions
func GetPathwaySubs(pathway string) Subscriptions {
	sub := Subscription{Pathway: pathway}
	return GetSubs(sub)
}
func HasBrokerSub(expression string) (bool, string) {
	sub := Subscription{Expression: expression, Topic: tukcnst.DSUB_TOPIC_TYPE_CODE}
	subs := GetSubs(sub)
	if subs.Count > 0 {
		for _, v := range subs.Subscriptions {
			if v.BrokerRef != "" {
				return true, subs.Subscriptions[1].BrokerRef
			}
		}
	}
	return false, ""
}
func HasUserSub(usersub Subscription) bool {
	subs := GetSubs(usersub)
	return subs.Count == 1
}
func GetSubs(sub Subscription) Subscriptions {
	subs := Subscriptions{Action: tukcnst.SELECT}
	subs.Subscriptions = append(subs.Subscriptions, sub)
	NewDBEvent(&subs)
	return subs
}
func NewSub(sub Subscription) error {
	subs := Subscriptions{Action: tukcnst.INSERT}
	subs.Subscriptions = append(subs.Subscriptions, sub)
	return NewDBEvent(&subs)
}
func CancelEsub(sub Subscription) Subscriptions {
	subs := Subscriptions{Action: tukcnst.DELETE}
	subs.Subscriptions = append(subs.Subscriptions, sub)
	usersub := Subscription{User: sub.User, Org: sub.Org, Role: sub.Role}
	return GetSubs(usersub)
}
func (i *Subscriptions) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_SUBSCRIPTIONS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Subscriptions) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.SUBSCRIPTIONS, reflectStruct(reflect.ValueOf(i.Subscriptions[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		for rows.Next() {
			sub := Subscription{}
			if err := rows.Scan(&sub.Id, &sub.Created, &sub.BrokerRef, &sub.Pathway, &sub.Topic, &sub.Expression, &sub.Email, &sub.NhsId, &sub.User, &sub.Org, &sub.Role); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Subscriptions = append(i.Subscriptions, sub)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Events
func GetTaskNotes(pwy string, nhsid string, taskid int, ver int) (string, error) {
	notes := ""
	evs := Events{Action: tukcnst.SELECT}
	ev := Event{Pathway: pwy, NhsId: nhsid, TaskId: taskid, Version: ver}
	evs.Events = append(evs.Events, ev)
	err := NewDBEvent(&evs)
	if err == nil && evs.Count > 0 {
		for _, note := range evs.Events {
			if note.Id != 0 {
				notes = notes + note.Comments + "\n"
			}
		}
		log.Printf("Found TaskId %v Notes %s", taskid, notes)
	}
	return notes, err
}
func (i *Events) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_EVENTS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Events) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.EVENTS, reflectStruct(reflect.ValueOf(i.Events[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}

		for rows.Next() {
			ev := Event{}
			if err := rows.Scan(&ev.Id, &ev.Creationtime, &ev.EventType, &ev.DocName, &ev.ClassCode, &ev.ConfCode, &ev.FormatCode, &ev.FacilityCode, &ev.PracticeCode, &ev.Speciality, &ev.Expression, &ev.Authors, &ev.XdsPid, &ev.XdsDocEntryUid, &ev.RepositoryUniqueId, &ev.NhsId, &ev.User, &ev.Org, &ev.Role, &ev.Topic, &ev.Pathway, &ev.Comments, &ev.Version, &ev.TaskId); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Events = append(i.Events, ev)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Workflows
func GetWorkflows(pathway string, nhsid string, version int, status string) (Workflows, error) {
	wfs := Workflows{Action: tukcnst.SELECT}
	wf := Workflow{Pathway: pathway, NHSId: nhsid, Version: version, Status: status}
	wfs.Workflows = append(wfs.Workflows, wf)
	err := wfs.newEvent()
	return wfs, err
}
func (i *Workflows) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_WORKFLOWS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Workflows) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.WORKFLOWS, reflectStruct(reflect.ValueOf(i.Workflows[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			workflow := Workflow{}
			if err := rows.Scan(&workflow.Id, &workflow.Pathway, &workflow.NHSId, &workflow.Created, &workflow.XDW_Key, &workflow.XDW_UID, &workflow.XDW_Doc, &workflow.XDW_Def, &workflow.Version, &workflow.Published, &workflow.Status); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Workflows = append(i.Workflows, workflow)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// XDWs
func GetPathways(user string) map[string]string {
	var names = make(map[string]string)
	xdws := XDWS{Action: tukcnst.SELECT}
	xdw := XDW{IsXDSMeta: false}
	xdws.XDW = append(xdws.XDW, xdw)
	if err := xdws.newEvent(); err == nil {
		for _, xdw := range xdws.XDW {
			if xdw.Id > 0 {
				names[xdw.Name] = strings.TrimSpace(GetIDMapsMappedId(user, xdw.Name))
			}
		}
	}
	log.Printf("%v Pathways Defined - %v", len(names), names)
	return names
}
func GetWorkflowDefinition(name string) (XDW, error) {
	var err error
	xdws := XDWS{Action: tukcnst.SELECT}
	xdw := XDW{Name: name}
	xdws.XDW = append(xdws.XDW, xdw)
	if err = xdws.newEvent(); err == nil {
		if xdws.Count == 1 {
			return xdws.XDW[1], nil
		} else {
			return xdw, errors.New("no xdw registered for " + name)
		}
	}
	return xdw, err
}
func GetWorkflowXDSMeta(name string) (string, error) {
	var err error
	xdws := XDWS{Action: tukcnst.SELECT}
	xdw := XDW{Name: name, IsXDSMeta: true}
	xdws.XDW = append(xdws.XDW, xdw)
	if err = xdws.newEvent(); err == nil {
		if xdws.Count == 1 {
			return xdws.XDW[1].XDW, nil
		}
	}
	return "", errors.New("no xdw meta registered for " + name)
}

func PersistWorkflowDefinition(name string, config string, isxdsmeta bool) error {
	xdws := XDWS{Action: tukcnst.DELETE}
	xdw := XDW{Name: name, IsXDSMeta: isxdsmeta}
	xdws.XDW = append(xdws.XDW, xdw)
	xdws.newEvent()
	xdws = XDWS{Action: tukcnst.INSERT}
	xdw = XDW{Name: name, IsXDSMeta: isxdsmeta, XDW: config}
	xdws.XDW = append(xdws.XDW, xdw)
	return xdws.newEvent()
}
func (i *XDWS) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_XDWS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.XDW) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.XDWS, reflectStruct(reflect.ValueOf(i.XDW[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			xdw := XDW{}
			if err := rows.Scan(&xdw.Id, &xdw.Name, &xdw.IsXDSMeta, &xdw.XDW); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.XDW = append(i.XDW, xdw)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Workflowstates
func (i *WorkflowStates) newEvent() error {
	var err error
	var stmntStr = "SELECT * FROM workflowstate"
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Workflowstate) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, "workflowstate", reflectStruct(reflect.ValueOf(i.Workflowstate[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			workflow := Workflowstate{}
			if err := rows.Scan(&workflow.Id, &workflow.WorkflowId, &workflow.Pathway, &workflow.NHSId, &workflow.Version, &workflow.Published, &workflow.Created, &workflow.CreatedBy, &workflow.Status, &workflow.CompleteBy, &workflow.LastUpdate, &workflow.Owner, &workflow.Overdue, &workflow.Escalated, &workflow.TargetMet, &workflow.InProgress, &workflow.Duration, &workflow.TimeRemaining); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Workflowstate = append(i.Workflowstate, workflow)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Templates
func PersistTemplate(user string, templatename string, templatestr string) error {
	tmplts := Templates{Action: tukcnst.DELETE}
	tmplt := Template{Name: templatename, User: user}
	tmplts.Templates = append(tmplts.Templates, tmplt)
	tmplts.newEvent()
	tmplts = Templates{Action: tukcnst.INSERT}
	tmplt = Template{Name: templatename, Template: templatestr}
	tmplts.Templates = append(tmplts.Templates, tmplt)
	return tmplts.newEvent()
}
func (i *Templates) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_TEMPLATES
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Templates) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.TEMPLATES, reflectStruct(reflect.ValueOf(i.Templates[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			tmplt := Template{}
			if err := rows.Scan(&tmplt.Id, &tmplt.Name, &tmplt.Template, &tmplt.User); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Templates = append(i.Templates, tmplt)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Idmaps
func GetIDMapsMappedId(user string, localid string) string {
	if user == "" {
		user = "system"
	}
	duration := time.Duration(1) * time.Minute
	expires := cached.Add(duration)
	if len(cachedIDMaps) == 0 || time.Now().After(expires) {
		idmaps := IdMaps{Action: tukcnst.SELECT}
		if err := idmaps.newEvent(); err != nil {
			log.Println(err.Error())
		}
		cachedIDMaps = idmaps.LidMap
		cached = time.Now()
	}
	for _, v := range cachedIDMaps {
		if v.User == user && v.Lid == localid {
			return v.Mid
		}
	}
	if user != "system" {
		user = "system"
		for _, v := range cachedIDMaps {
			if v.User == user && v.Lid == localid {
				return v.Mid
			}
		}
	}
	return localid
}
func GetIDMapsLocalId(user string, mid string) string {
	if user == "" {
		user = "system"
	}
	idmaps := IdMaps{Action: tukcnst.SELECT}
	idmap := IdMap{User: user}
	idmaps.LidMap = append(idmaps.LidMap, idmap)
	if err := idmaps.newEvent(); err != nil {
		log.Println(err.Error())
	}
	for _, idmap := range idmaps.LidMap {
		if idmap.Mid == mid && idmap.User == user {
			return idmap.Lid
		}
	}
	return mid
}
func (i *IdMaps) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_IDMAPS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.LidMap) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.ID_MAPS, reflectStruct(reflect.ValueOf(i.LidMap[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			idmap := IdMap{}
			if err := rows.Scan(&idmap.Id, &idmap.Lid, &idmap.Mid, &idmap.User); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.LidMap = append(i.LidMap, idmap)
			i.Cnt = i.Cnt + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

// Statics
func (i *Statics) newEvent() error {
	var err error
	var stmntStr = tukcnst.SQL_DEFAULT_STATICS
	var rows *sql.Rows
	var vals []interface{}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	if len(i.Static) > 0 {
		if stmntStr, vals, err = createPreparedStmnt(i.Action, tukcnst.STATICS, reflectStruct(reflect.ValueOf(i.Static[0]))); err != nil {
			log.Println(err.Error())
			return err
		}
	}
	sqlStmnt, err := DBConn.PrepareContext(ctx, stmntStr)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer sqlStmnt.Close()

	if i.Action == tukcnst.SELECT {
		rows, err = setRows(ctx, sqlStmnt, vals)
		if err != nil {
			log.Println(err.Error())
			return err
		}
		for rows.Next() {
			s := Static{}
			if err := rows.Scan(&s.Id, &s.Name, &s.Content); err != nil {
				switch {
				case err == sql.ErrNoRows:
					return nil
				default:
					log.Println(err.Error())
					return err
				}
			}
			i.Static = append(i.Static, s)
			i.Count = i.Count + 1
		}
	} else {
		i.LastInsertId, err = setLastID(ctx, sqlStmnt, vals)
	}
	return err
}

func reflectStruct(i reflect.Value) map[string]interface{} {
	params := make(map[string]interface{})
	structType := i.Type()
	for f := 0; f < i.NumField(); f++ {
		field := structType.Field(f)
		fieldName := field.Name
		fieldType := field.Type
		switch fieldType.Kind() {
		case reflect.Int:
			val := i.Field(f).Interface().(int)
			if (strings.ToLower(fieldName) == "taskid" && val > -1) || (strings.ToLower(fieldName) != "taskid" && val > 0) {
				params[strings.ToLower(fieldName)] = val
				log.Printf("Reflected param %s : value %v", strings.ToLower(fieldName), val)
			}
		case reflect.Bool:
			val := i.Field(f).Interface().(bool)
			params[strings.ToLower(fieldName)] = val
			log.Printf("Reflected param %s : value %v", strings.ToLower(fieldName), val)
		case reflect.String:
			val := i.Field(f).Interface().(string)
			if len(val) > 0 {
				params[strings.ToLower(fieldName)] = val
				if len(i.Field(f).Interface().(string)) > 100 {
					log.Printf("Reflected param %s : value (Truncated first 50 chars) %v", strings.ToLower(fieldName), i.Field(f).Interface().(string)[0:50])
				} else {
					log.Printf("Reflected param %s : value %s", strings.ToLower(fieldName), i.Field(f).Interface().(string))
				}
			}
		default:
			log.Printf("Field %s has an unknown type %v\n", fieldName, fieldType.Kind())
		}

		// if strings.EqualFold(fieldName, "version") || strings.EqualFold(fieldName, "LastInsertId") || strings.EqualFold(fieldName, "Id") || strings.EqualFold(fieldName, "TaskId") || strings.EqualFold(fieldName, "WorkflowId") {
		// 	tint := i.Field(f).Interface().(int)
		// 	if tint > 0 {
		// 		params[strings.ToLower(fieldName)] = tint
		// 		log.Printf("Reflected param %s : value %v", strings.ToLower(fieldName), tint)
		// 	}
		// } else {
		// 	if strings.EqualFold(fieldName, "published") || strings.EqualFold(fieldName, "isxdsmeta") {
		// 		params[strings.ToLower(fieldName)] = i.Field(f).Interface()
		// 		log.Printf("Reflected param %s : value %v", strings.ToLower(fieldName), i.Field(f).Interface())
		// 	} else {
		// 		if i.Field(f).Interface() != nil && len(i.Field(f).Interface().(string)) > 0 {
		// 			params[strings.ToLower(fieldName)] = i.Field(f).Interface()
		// 			log.Printf("Reflected param %s : value %v", strings.ToLower(fieldName), i.Field(f).Interface())
		// 		}
		// 	}
		// }
	}
	return params
}
func createPreparedStmnt(action string, table string, params map[string]interface{}) (string, []interface{}, error) {
	var vals []interface{}
	stmntStr := "SELECT * FROM " + table
	if len(params) > 0 {
		switch action {
		case tukcnst.SELECT:
			var paramStr string
			stmntStr = stmntStr + " WHERE "
			for param, val := range params {
				paramStr = paramStr + param + "= ? AND "
				vals = append(vals, val)
			}
			paramStr = strings.TrimSuffix(paramStr, " AND ")
			stmntStr = stmntStr + paramStr
		case tukcnst.INSERT:
			var paramStr string
			var qStr string
			stmntStr = "INSERT INTO " + table + " ("
			for param, val := range params {
				paramStr = paramStr + param + ", "
				qStr = qStr + "?, "
				vals = append(vals, val)
			}
			paramStr = strings.TrimSuffix(paramStr, ", ") + ") VALUES ("
			qStr = strings.TrimSuffix(qStr, ", ")
			stmntStr = stmntStr + paramStr + qStr + ")"
		case tukcnst.DEPRECATE:
			switch table {
			case tukcnst.WORKFLOWS:
				stmntStr = "UPDATE workflows SET version = version + 1 WHERE xdw_key=?"
				vals = append(vals, params["xdw_key"])
			case tukcnst.EVENTS:
				stmntStr = "UPDATE events SET version = version + 1 WHERE pathway=? AND nhsid=?"
				vals = append(vals, params["pathway"])
				vals = append(vals, params["nhsid"])
			}
		case tukcnst.UPDATE:
			switch table {
			case tukcnst.WORKFLOWS:
				stmntStr = "UPDATE workflows SET xdw_doc = ?, published = ?, status = ? WHERE pathway = ? AND nhsid = ? AND version = ?"
				vals = append(vals, params["xdw_doc"])
				vals = append(vals, params["published"])
				vals = append(vals, params["status"])
				vals = append(vals, params["pathway"])
				vals = append(vals, params["nhsid"])
				vals = append(vals, params["version"])
			case tukcnst.ID_MAPS:
				stmntStr = "UPDATE idmaps SET "
				var paramStr string
				for param, val := range params {
					if val != "" && param != "id" {
						paramStr = paramStr + param + "= ?, "
						vals = append(vals, val)
					}
				}
				vals = append(vals, params["id"])
				paramStr = strings.TrimSuffix(paramStr, ", ")
				stmntStr = stmntStr + paramStr + " WHERE id = ?"
			}
		case tukcnst.DELETE:
			stmntStr = "DELETE FROM " + table + " WHERE "
			var paramStr string
			for param, val := range params {
				paramStr = paramStr + param + "= ? AND "
				vals = append(vals, val)
			}
			paramStr = strings.TrimSuffix(paramStr, " AND ")
			stmntStr = stmntStr + paramStr
		}
		log.Printf("Created Prepared Statement %s - Values %s", stmntStr, vals)
	}
	return stmntStr, vals, nil
}
func setRows(ctx context.Context, sqlStmnt *sql.Stmt, vals []interface{}) (*sql.Rows, error) {
	if len(vals) > 0 {
		return sqlStmnt.QueryContext(ctx, vals...)
	} else {
		return sqlStmnt.QueryContext(ctx)
	}
}
func setLastID(ctx context.Context, sqlStmnt *sql.Stmt, vals []interface{}) (int, error) {
	if len(vals) > 0 {
		sqlrslt, err := sqlStmnt.ExecContext(ctx, vals...)
		if err != nil {
			log.Println(err.Error())
			return 0, err
		}
		id, err := sqlrslt.LastInsertId()
		if err != nil {
			log.Println(err.Error())
			return 0, err
		} else {
			return int(id), nil
		}
	}
	return 0, nil
}
