package clouddriveclient

import (
	"io"
	"time"
)

type Endpoint struct {
	CustomerExists bool   `json:"customerExists"`
	ContentUrl     string `json:"contentUrl"`
	MetadataUrl    string `json:"metadataUrl"`
}

const (
	NodeKindAsset  = "ASSET"
	NodeKindFile   = "FILE"
	NodeKindFolder = "FOLDER"
	NodeKindGroup  = "GROUP"
)

const (
	NodeStatusAvailable = "AVAILABLE"
	NodeStatusPending   = "PENDING"
	NodeStatusTrash     = "TRASH"
	NodeStatusPurged    = "PURGED"
)

type Node struct {
	Id                string                `json:"id"`
	Name              string                `json:"name"`
	Kind              string                `json:"kind"`
	Parents           []string              `json:"parents"`
	Status            string                `json:"status"`
	ModifiedDate      time.Time             `json:"modifiedDate"`
	ContentProperties NodeContentProperties `json:"contentProperties"`
	Reader            io.ReadCloser
}

type NodeContentProperties struct {
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
	Md5         string `json:"md5"`
}

type Nodes struct {
	Nodes     []*Node `json:"data"`
	Count     int     `json:"count"`
	NextToken string  `json:"nextToken"`
}

type NodeCreate struct {
	Name    string   `json:"name"`
	Kind    string   `json:"kind"`
	Parents []string `json:"parents"`
}

type NodeRename struct {
	Name string `json:"name"`
}

type NodeMove struct {
	FromParent string `json:"fromParent"`
	ChildId    string `json:"childId"`
}

type Quota struct {
	Quota          int64     `json:"quota"`
	LastCalculated time.Time `json:"lastCalculated"`
	Available      int64     `json:"available"`
}

type Changes struct {
	Checkpoint string  `json:"checkpoint"`
	Nodes      []*Node `json:"nodes"`
	Reset      bool    `json:"reset"`
}
