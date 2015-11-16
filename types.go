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

type RefreshResp struct {
	ExpiresIn   int64  `json:"expires_in"`
	AccessToken string `json:"access_token"`
}

const NodeKindFile = "FILE"
const NodeKindFolder = "FOLDER"

type Node struct {
	Id                string                `json:"id"`
	Name              string                `json:"name"`
	Kind              string                `json:"kind"`
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
