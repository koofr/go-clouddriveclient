package clouddriveclient

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/koofr/go-httpclient"
	"github.com/koofr/go-ioutils"
)

const DefaultMaxRetries = 5

type CloudDrive struct {
	ContentClient  *httpclient.HTTPClient
	MetadataClient *httpclient.HTTPClient
	Auth           *CloudDriveAuth
	MaxRetries     int
}

func NewCloudDrive(auth *CloudDriveAuth) (d *CloudDrive, err error) {
	d = &CloudDrive{
		Auth:       auth,
		MaxRetries: DefaultMaxRetries,
	}

	endpointUrl, _ := url.Parse("https://drive.amazonaws.com/drive/v1")

	endpointClient := httpclient.New()
	endpointClient.BaseURL = endpointUrl

	endpoint := &Endpoint{}

	endpointReq := &httpclient.RequestData{
		Method:         "GET",
		Path:           "/account/endpoint",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &endpoint,
	}

	_, err = d.Request(endpointClient, endpointReq)

	if err != nil {
		return nil, err
	}

	if !endpoint.CustomerExists {
		return nil, fmt.Errorf("CloudDrive customer does not exist.")
	}

	contentUrl, _ := url.Parse(endpoint.ContentUrl)
	metadataUrl, _ := url.Parse(endpoint.MetadataUrl)

	d.ContentClient = httpclient.New()
	d.ContentClient.BaseURL = contentUrl

	d.MetadataClient = httpclient.New()
	d.MetadataClient.BaseURL = metadataUrl

	return d, nil
}

func (d *CloudDrive) HandleError(err error) error {
	return HandleError(err)
}

func (d *CloudDrive) Request(client *httpclient.HTTPClient, request *httpclient.RequestData) (response *http.Response, err error) {
	retries := d.MaxRetries

	canRetry := request.CanCopy()

	if !canRetry {
		retries = 1
	}

	for retry := 0; retry < retries; retry++ {
		var currentRequest *httpclient.RequestData

		if canRetry {
			_, currentRequest = request.Copy()
		} else {
			currentRequest = request
		}

		token, err := d.Auth.ValidToken()
		if err != nil {
			return nil, err
		}

		if currentRequest.Headers == nil {
			currentRequest.Headers = http.Header{}
		}

		currentRequest.Headers.Set("Authorization", "Bearer "+token)

		response, err = client.Request(currentRequest)

		doRetry := false

		if err != nil {
			if httpErr, ok := err.(httpclient.InvalidStatusError); ok {
				doRetry = httpErr.Got == 429
			}
		}

		if doRetry {
			seconds := rand.Intn(int(math.Pow(2, float64(retry))))

			time.Sleep(time.Duration(seconds) * time.Second)

			continue
		}

		return response, d.HandleError(err)
	}

	return nil, fmt.Errorf("Too many retries")
}

func (d *CloudDrive) LookupRoot() (root *Node, err error) {
	params := make(url.Values)
	params.Set("filters", "isRoot:true")

	nodes := &Nodes{}

	req := &httpclient.RequestData{
		Method:         "GET",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &nodes,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	if len(nodes.Nodes) == 0 {
		return nil, fmt.Errorf("Root not found.")
	}

	root = nodes.Nodes[0]

	return root, nil
}

func (d *CloudDrive) LookupNode(parentId string, name string) (node *Node, ok bool, err error) {
	nameEscaped := strings.Replace(name, "\"", "\\\\", -1)

	params := make(url.Values)
	params.Set("filters", "parents:"+parentId+" AND name:\""+nameEscaped+"\"")

	nodes := &Nodes{}

	req := &httpclient.RequestData{
		Method:         "GET",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &nodes,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, false, err
	}

	if len(nodes.Nodes) == 0 {
		return nil, false, nil
	}

	return nodes.Nodes[0], true, nil
}

func (d *CloudDrive) NodeChildren(parentId string) (nodes []*Node, err error) {
	nextToken := ""

	nodes = []*Node{}

	for {
		params := make(url.Values)
		if nextToken != "" {
			params.Set("startToken", nextToken)
		}

		ns := &Nodes{}

		req := &httpclient.RequestData{
			Method:         "GET",
			Path:           "/nodes/" + parentId + "/children",
			Params:         params,
			ExpectedStatus: []int{http.StatusOK},
			RespEncoding:   httpclient.EncodingJSON,
			RespValue:      &ns,
		}

		_, err = d.Request(d.MetadataClient, req)

		if err != nil {
			return nil, err
		}

		if len(ns.Nodes) == 0 {
			break
		}

		nodes = append(nodes, ns.Nodes...)

		if ns.NextToken == "" {
			break
		}

		nextToken = ns.NextToken
	}

	return nodes, nil
}

func (d *CloudDrive) Changes(checkpoint string) (changes *Changes, err error) {
	req := &httpclient.RequestData{
		Method:         "POST",
		Path:           "/changes",
		ExpectedStatus: []int{http.StatusOK},
	}

	if checkpoint != "" {
		req.ReqEncoding = httpclient.EncodingJSON

		req.ReqValue = struct {
			Checkpoint string `json:"checkpoint"`
		}{
			Checkpoint: checkpoint,
		}
	}

	res, err := d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	var r io.ReadCloser = res.Body

	if res.Header.Get("Content-Encoding") == "gzip" {
		r, err = gzip.NewReader(r)

		if err != nil {
			return nil, err
		}
	}

	decoder := json.NewDecoder(r)

	changes = &Changes{}

	err = decoder.Decode(changes)

	if err != nil {
		return nil, err
	}

	res.Body.Close()

	return changes, nil
}

func (d *CloudDrive) CreateFolder(parentId string, name string) (node *Node, err error) {
	create := &NodeCreate{
		Name:    name,
		Kind:    NodeKindFolder,
		Parents: []string{parentId},
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "POST",
		Path:           "/nodes",
		ExpectedStatus: []int{http.StatusCreated},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       create,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) DeleteNode(nodeId string) (node *Node, err error) {
	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "PUT",
		Path:           "/trash/" + nodeId,
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) RenameNode(nodeId string, newName string) (node *Node, err error) {
	rename := &NodeRename{
		Name: newName,
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "PATCH",
		Path:           "/nodes/" + nodeId,
		ExpectedStatus: []int{http.StatusOK},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       rename,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) MoveNode(nodeId string, fromParentId string, toParentId string) (node *Node, err error) {
	move := &NodeMove{
		FromParent: fromParentId,
		ChildId:    nodeId,
	}

	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "POST",
		Path:           "/nodes/" + toParentId + "/children",
		ExpectedStatus: []int{http.StatusOK},
		ReqEncoding:    httpclient.EncodingJSON,
		ReqValue:       move,
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) DownloadNode(nodeId string, span *ioutils.FileSpan) (reader io.ReadCloser, size int64, err error) {
	req := &httpclient.RequestData{
		Method:         "GET",
		Path:           "/nodes/" + nodeId + "/content",
		ExpectedStatus: []int{http.StatusOK, http.StatusPartialContent},
	}

	if span != nil {
		req.Headers = make(http.Header)
		req.Headers.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Start, span.End))
	}

	if err != nil {
		return nil, 0, err
	}

	res, err := d.Request(d.ContentClient, req)

	if err != nil {
		return nil, 0, err
	}

	return res.Body, res.ContentLength, nil
}

func (d *CloudDrive) UploadNode(parentId string, name string, reader io.Reader) (node *Node, err error) {
	create := &NodeCreate{
		Name:    name,
		Kind:    NodeKindFile,
		Parents: []string{parentId},
	}

	createJson, err := json.Marshal(create)

	if err != nil {
		return nil, err
	}

	params := make(url.Values)
	params.Set("suppress", "deduplication")

	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "POST",
		Path:           "/nodes",
		Params:         params,
		ExpectedStatus: []int{http.StatusCreated},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	extra := map[string]string{
		"metadata": string(createJson),
	}

	err = req.UploadFileExtra("file", "file", reader, extra)

	if err != nil {
		return nil, err
	}

	_, err = d.Request(d.ContentClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) OverwriteNode(nodeId string, reader io.Reader) (node *Node, err error) {
	node = &Node{}

	req := &httpclient.RequestData{
		Method:         "PUT",
		Path:           "/nodes/" + nodeId + "/content",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &node,
	}

	err = req.UploadFile("file", "file", reader)

	if err != nil {
		return nil, err
	}

	_, err = d.Request(d.ContentClient, req)

	if err != nil {
		return nil, err
	}

	return node, nil
}

func (d *CloudDrive) Quota() (quota *Quota, err error) {
	quota = &Quota{}

	req := &httpclient.RequestData{
		Method:         "GET",
		Path:           "/account/quota",
		ExpectedStatus: []int{http.StatusOK},
		RespEncoding:   httpclient.EncodingJSON,
		RespValue:      &quota,
	}

	_, err = d.Request(d.MetadataClient, req)

	if err != nil {
		return nil, err
	}

	return quota, nil
}
